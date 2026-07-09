#!/usr/bin/env python3
"""Generated property for the fail-loud schema gate (deploy-kill-migrate-wants
fix, see nix/module.nix + lib/migrator.py::assert_schema_current).

**Invariant (INV-2):** the pipeline never runs a cycle against a DB missing
any shipped migration. ``cratedigger.service`` and
``cratedigger-unfindable.service`` dropped their ``Requires=`` edge on
``cratedigger-db-migrate.service`` so a switch-time migrate restart can't
SIGTERM a mid-flight cycle (systemd ``Requires=`` stop-propagation); losing
that edge loses the "a failed/behind migration blocks the app" guarantee,
so ``lib.migrator.missing_migration_versions`` (the pure decision behind
``assert_schema_current``) re-provides it as a startup check both units
call. INV-1 (the unit wiring itself) is Nix config with no
generated-testable surface -- it ships with a deterministic pin only, in
``nix/tests/module-vm.nix``.

Deterministic pin half of the PAIR: ``TestMissingMigrationVersions`` in
tests/test_migrator.py. Known-bad self-test for the checker: below.
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)

from hypothesis import example, given
from hypothesis import strategies as st

from lib.migrator import missing_migration_versions

_VERSION_UNIVERSE = st.integers(min_value=1, max_value=9999)


@st.composite
def _version_set_pairs(draw) -> tuple[set[int], set[int]]:
    applied = draw(st.sets(_VERSION_UNIVERSE, max_size=20))
    shipped = draw(st.sets(_VERSION_UNIVERSE, max_size=20))
    return applied, shipped


def assert_missing_versions_invariant(
    applied: set[int], shipped: set[int], result: list[int],
) -> None:
    """Module-level checker (known-bad self-tests below): the gate must
    flag exactly ``shipped - applied``, sorted ascending."""
    expected = sorted(shipped - applied)
    if result != expected:
        raise AssertionError(
            f"missing_migration_versions diverged: applied={applied} "
            f"shipped={shipped} expected={expected} actual={result}")


class TestGeneratedMissingMigrationVersions(unittest.TestCase):
    """The gate flags exactly shipped-minus-applied over the version-set
    space -- fully current, fresh (nothing applied), and applied-ahead-of-
    shipped are all corners of the same strategy."""

    @given(pair=_version_set_pairs())
    @example(pair=(set(), set()))
    @example(pair=({1, 2, 3}, {1, 2, 3}))       # fully current
    @example(pair=({1, 2}, {1, 2, 3}))          # one behind
    @example(pair=(set(), {1, 2, 3}))           # fresh DB, nothing applied
    @example(pair=({1, 2, 3, 4}, {1, 2, 3}))    # applied ahead of shipped
    def test_missing_is_exactly_shipped_minus_applied(self, pair):
        applied, shipped = pair
        result = missing_migration_versions(applied, shipped)
        assert_missing_versions_invariant(applied, shipped, result)


class TestMissingVersionsCheckerTripsOnViolations(unittest.TestCase):
    """Known-bad self-tests: each planted violation must trip the checker."""

    def test_trips_when_a_missing_version_is_omitted(self):
        with self.assertRaises(AssertionError):
            assert_missing_versions_invariant({1}, {1, 2, 3}, [3])

    def test_trips_when_an_applied_version_is_wrongly_flagged_missing(self):
        with self.assertRaises(AssertionError):
            assert_missing_versions_invariant({1, 2, 3}, {1, 2, 3}, [2])

    def test_trips_when_result_is_not_sorted(self):
        with self.assertRaises(AssertionError):
            assert_missing_versions_invariant(set(), {3, 1, 2}, [3, 1, 2])

    def test_trips_when_result_has_a_phantom_version(self):
        with self.assertRaises(AssertionError):
            assert_missing_versions_invariant(set(), {1, 2}, [1, 2, 99])


if __name__ == "__main__":
    unittest.main()
