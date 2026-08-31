"""Read-projection parity completeness audit (issue #546 W1).

The read-side mirror of ``tests/test_pipeline_db_write_audit.py``. Where
the write audit makes every ``PipelineDB`` write method carry a Rule A
round-trip test, this audit makes every ``FakePipelineDB`` READ
projection (``get_*`` / ``list_*``) carry a fake<->production key-set
parity check — OR be explicitly allowlisted with a one-line rationale.

The authoritative universe is ``enumerate_read_mirrors()`` (every public
read method on ``FakePipelineDB``). Each mirror MUST fall into exactly
one bucket:

  (a) a key in ``PARITY_REGISTRY`` — a registry seeder drives it through
      ``TestReadProjectionRegistryParity``;
  (b) a hand-written parity test in ``tests/test_pipeline_db.py`` that
      calls ``db.<method>()`` inside a parity-shaped test (detected by
      AST across ALL classes in that file); or
  (c) a key in ``ALLOWLIST`` — no raw ``SELECT`` row projection to
      key-compare (typed Struct return, scalar, or computed metric dict).

A mirror in none of the three fails the audit with actionable guidance:
add a registry seeder or an allowlist entry. This makes the whole read
surface self-enforcing — a new ``FakePipelineDB`` read method can't ship
without landing in a bucket, so #524/#525-class drift can't recur.

Like the write audit, ``tests/test_pipeline_db.py`` is parsed as TEXT
via ``ast`` (path derived from ``__file__``) — never imported, because
importing it bootstraps ephemeral PostgreSQL.
"""
from __future__ import annotations

import ast
import pathlib
import unittest

from tests.read_projection_registry import (
    ALLOWLIST,
    PARITY_REGISTRY,
    VALUE_PARITY_REGISTRY,
    ValueExclusion,
    compare_projection_values,
    enumerate_read_mirrors,
)


def _find_parity_tests_for_method(
    method_name: str, tree: ast.Module,
) -> list[str]:
    """Return parity test functions in ``tree`` that exercise ``method_name``.

    A test qualifies as a parity test for ``method_name`` when its body
    either:

      * calls ``<x>.<method_name>(...)`` at least TWICE (once per backend —
        the ``self.db`` real-PG call and the ``self.fake`` / ``fake`` call
        every hand-written parity test makes), OR
      * passes ``<method_name>``'s result INTO ``_assert_keyset_parity(...)``
        — either a direct inline ``_assert_keyset_parity(self, db.M(), ...)``
        call, or a variable bound from a call to ``M`` and then passed as an
        argument.

    Pure name-based acceptance (``"parity" in test.name``) is deliberately
    REJECTED — it let a name-only smoke test or a setup-only mention count
    as coverage, looser than the write-side sibling
    (``tests/test_pipeline_db_write_audit.py``). Requiring two real call
    sites (or an actual result-into-comparator flow) keeps the heuristic
    strictly tighter while still registering all 13 existing hand-covered
    methods across ``TestReadProjectionParity``, ``TestGetWrongMatches``,
    ``TestGetPipelineOverlay``, ``TestSlskdEventCursorRoundTrip`` and
    ``TestGetDownloadLogCounts``.
    """

    def _is_call_to_method(sub: ast.AST) -> bool:
        return (
            isinstance(sub, ast.Call)
            and isinstance(sub.func, ast.Attribute)
            and sub.func.attr == method_name
        )

    hits: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef):
            continue
        if not node.name.startswith("test_"):
            continue

        # (a) Count call sites of ``<x>.<method_name>(...)``.
        call_count = sum(
            1 for sub in ast.walk(node) if _is_call_to_method(sub))

        # Names bound from an expression that contains a call to ``M``
        # (covers ``rows = db.M()`` and ``rows = list(db.M())``).
        m_bound_names: set[str] = set()
        for sub in ast.walk(node):
            if isinstance(sub, ast.Assign) and any(
                _is_call_to_method(v) for v in ast.walk(sub.value)
            ):
                for target in sub.targets:
                    if isinstance(target, ast.Name):
                        m_bound_names.add(target.id)

        # (b) Is ``M``'s result passed into ``_assert_keyset_parity(...)``?
        passed_into_assert = False
        for sub in ast.walk(node):
            if not (
                isinstance(sub, ast.Call)
                and isinstance(sub.func, ast.Attribute)
                and sub.func.attr == "_assert_keyset_parity"
            ):
                continue
            for arg in sub.args:
                if _is_call_to_method(arg):
                    passed_into_assert = True
                if isinstance(arg, ast.Name) and arg.id in m_bound_names:
                    passed_into_assert = True

        if call_count >= 2 or passed_into_assert:
            hits.append(node.name)
    return hits


class TestReadProjectionAudit(unittest.TestCase):
    """Every ``FakePipelineDB`` read mirror lands in exactly one bucket.

    Add a new ``get_*`` / ``list_*`` / ``search_*`` / ``find_*`` /
    ``fetch_*`` method to ``FakePipelineDB`` ⇒ the audit fails until you
    either:
      1. Add a registry seeder in ``tests/read_projection_registry.py``
         (preferred for any raw-SELECT row projection), OR
      2. Add an ``ALLOWLIST`` entry there with a one-line rationale (for
         typed Struct / scalar / computed-metric returns), OR
      3. Add a hand-written parity test in ``tests/test_pipeline_db.py``.

    There is no fourth option — a new read mirror without a bucket is the
    #523 drift class waiting to recur.
    """

    @classmethod
    def setUpClass(cls) -> None:
        # Parse tests/test_pipeline_db.py once as TEXT — never import it
        # (that bootstraps ephemeral PostgreSQL). Derive the path from this
        # file's own location so it survives lib.pipeline_db being a package.
        test_path = (
            pathlib.Path(__file__).resolve().parent / "test_pipeline_db.py"
        )
        cls._test_tree = ast.parse(test_path.read_text())

    def test_every_read_mirror_is_covered(self) -> None:
        uncovered: list[str] = []
        for name in enumerate_read_mirrors():
            if name in PARITY_REGISTRY:
                continue
            if name in ALLOWLIST:
                continue
            if _find_parity_tests_for_method(name, self._test_tree):
                continue
            uncovered.append(name)
        self.assertEqual(
            uncovered, [],
            msg=(
                "These FakePipelineDB read mirrors have no fake<->production "
                "key-set parity coverage (#546 W1). For each, EITHER add a "
                "registry seeder to PARITY_REGISTRY in "
                "tests/read_projection_registry.py (preferred for a raw "
                "SELECT row projection) OR add an ALLOWLIST entry there with "
                "a one-line rationale (typed Struct / scalar / computed "
                "metric return):\n  - " + "\n  - ".join(sorted(uncovered))
            ),
        )

    def test_registry_entries_match_real_methods(self) -> None:
        """Catch stale registry keys — a method renamed or deleted but
        left with a dangling seeder."""
        real_methods = set(enumerate_read_mirrors())
        stale = [name for name in PARITY_REGISTRY if name not in real_methods]
        self.assertEqual(
            stale, [],
            msg=(
                "PARITY_REGISTRY contains stale entries that don't match any "
                "current FakePipelineDB read method:\n  - "
                + "\n  - ".join(sorted(stale))
            ),
        )

    def test_allowlist_entries_match_real_methods(self) -> None:
        """Catch stale allowlist entries — a method renamed or deleted but
        left with a dangling allowlist row."""
        real_methods = set(enumerate_read_mirrors())
        stale = [name for name in ALLOWLIST if name not in real_methods]
        self.assertEqual(
            stale, [],
            msg=(
                "ALLOWLIST contains stale entries that don't match any "
                "current FakePipelineDB read method:\n  - "
                + "\n  - ".join(sorted(stale))
            ),
        )

    def test_allowlist_rationales_are_non_empty(self) -> None:
        empty = [name for name, reason in ALLOWLIST.items()
                 if not reason.strip()]
        self.assertEqual(
            empty, [],
            msg=(
                "ALLOWLIST entries must carry a one-line rationale:\n  - "
                + "\n  - ".join(sorted(empty))
            ),
        )

    def test_no_method_both_allowlisted_and_registered(self) -> None:
        """A method must be in exactly one of PARITY_REGISTRY / ALLOWLIST —
        never both (that would be contradictory bucketing)."""
        both = sorted(set(PARITY_REGISTRY) & set(ALLOWLIST))
        self.assertEqual(
            both, [],
            msg=(
                "These methods are in BOTH PARITY_REGISTRY and ALLOWLIST — "
                "pick one bucket (a seeded parity check OR an allowlist "
                "rationale):\n  - " + "\n  - ".join(both)
            ),
        )


class TestValueParityRegistry(unittest.TestCase):
    """#1278 item 7 — the VALUE registry's own shape.

    The value gate runs against real PostgreSQL in
    ``tests/test_pipeline_db.py::TestReadProjectionValueParity``; what is
    checkable without a database is that its keys name real mirrors and
    that no field leaves the comparison without a written reason.
    """

    def test_value_registry_entries_match_real_methods(self) -> None:
        real_methods = set(enumerate_read_mirrors())
        stale = [
            name for name in VALUE_PARITY_REGISTRY
            if name not in real_methods
        ]
        self.assertEqual(
            stale, [],
            msg=(
                "VALUE_PARITY_REGISTRY contains entries that don't match "
                "any current FakePipelineDB read method:\n  - "
                + "\n  - ".join(sorted(stale))
            ),
        )

    def test_every_shipped_exclusion_carries_a_rationale(self) -> None:
        bare = [
            f"{method}:{exclusion.path}"
            for method, entry in VALUE_PARITY_REGISTRY.items()
            for exclusion in entry.exclusions
            if not exclusion.rationale.strip()
        ]
        self.assertEqual(bare, [])

    def test_an_exclusion_without_a_rationale_cannot_be_written(self) -> None:
        with self.assertRaisesRegex(ValueError, "needs a one-line rationale"):
            ValueExclusion("totals.known_peers", "   ")

    def test_an_exclusion_without_a_path_cannot_be_written(self) -> None:
        with self.assertRaisesRegex(ValueError, "needs a field path"):
            ValueExclusion("", "some reason")


class TestValueComparator(unittest.TestCase):
    """Known-bad self-tests for ``compare_projection_values``.

    A comparator that has never caught anything is unfalsifiable, and this
    one decides whether the value gate means anything at all. One test per
    thing it claims to detect, plus the two ways it can be hollowed out
    (an exclusion that silently covers a sibling; a payload with nothing
    substantive in it).
    """

    def test_identical_rows_produce_no_mismatch(self) -> None:
        rows = [{"count": 3, "label": "x", "nested": {"inner": [1, 2]}}]
        result = compare_projection_values(
            rows, [dict(rows[0])], excluded=frozenset())
        self.assertEqual(result.mismatches, ())
        self.assertGreaterEqual(result.substantive_leaves, 1)

    def test_a_differing_leaf_is_reported_with_its_path(self) -> None:
        result = compare_projection_values(
            [{"totals": {"known_peers": 4}}],
            [{"totals": {"known_peers": 5}}],
            excluded=frozenset())
        self.assertEqual(len(result.mismatches), 1)
        self.assertIn("totals.known_peers", result.mismatches[0])
        self.assertIn("4", result.mismatches[0])
        self.assertIn("5", result.mismatches[0])

    def test_a_row_count_difference_is_reported(self) -> None:
        result = compare_projection_values(
            [{"a": 1}], [{"a": 1}, {"a": 2}], excluded=frozenset())
        self.assertTrue(
            any("row count" in m for m in result.mismatches),
            result.mismatches)

    def test_a_key_set_difference_is_reported(self) -> None:
        result = compare_projection_values(
            [{"a": 1, "only_real": 2}], [{"a": 1, "only_fake": 2}],
            excluded=frozenset())
        self.assertTrue(
            any("key sets differ" in m for m in result.mismatches),
            result.mismatches)

    def test_a_nested_list_length_difference_is_reported(self) -> None:
        result = compare_projection_values(
            [{"days": [{"n": 1}, {"n": 2}]}], [{"days": [{"n": 1}]}],
            excluded=frozenset())
        self.assertTrue(
            any("days: real PG returned 2 element(s)" in m
                for m in result.mismatches),
            result.mismatches)

    def test_a_container_vs_scalar_difference_is_reported(self) -> None:
        result = compare_projection_values(
            [{"validation_result": {"scenario": "x"}}],
            [{"validation_result": '{"scenario": "x"}'}],
            excluded=frozenset())
        self.assertTrue(
            any("returned dict" in m and "returned str" in m
                for m in result.mismatches),
            result.mismatches)

    def test_an_excluded_path_is_not_compared(self) -> None:
        result = compare_projection_values(
            [{"id": 1, "kept": "same"}], [{"id": 900, "kept": "same"}],
            excluded=frozenset({"id"}))
        self.assertEqual(result.mismatches, ())
        self.assertEqual(result.compared_leaves, 1)

    def test_an_exclusion_covers_every_element_of_a_list(self) -> None:
        """``[]`` means every element — an exclusion pinned to index 0 only
        would let a later element's drift through unseen."""
        result = compare_projection_values(
            [{"days": [{"at": "t0", "n": 1}, {"at": "t1", "n": 2}]}],
            [{"days": [{"at": "t9", "n": 1}, {"at": "t8", "n": 2}]}],
            excluded=frozenset({"days[].at"}))
        self.assertEqual(result.mismatches, ())
        self.assertEqual(result.compared_leaves, 2)

    def test_an_exclusion_does_not_cover_a_sibling_field(self) -> None:
        result = compare_projection_values(
            [{"totals": {"known_peers": 4, "new_24h": 1}}],
            [{"totals": {"known_peers": 9, "new_24h": 7}}],
            excluded=frozenset({"totals.known_peers"}))
        self.assertEqual(len(result.mismatches), 1)
        self.assertIn("totals.new_24h", result.mismatches[0])

    def test_a_payload_of_nulls_zeros_and_empties_is_not_substantive(
        self,
    ) -> None:
        rows = [{"a": None, "b": 0, "c": "", "d": [], "e": False}]
        result = compare_projection_values(
            rows, [dict(rows[0])], excluded=frozenset())
        self.assertEqual(result.mismatches, ())
        self.assertEqual(result.substantive_leaves, 0)


if __name__ == "__main__":
    unittest.main()
