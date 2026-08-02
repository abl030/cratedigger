"""Contracts for explicit, proved finite generated domains."""

from __future__ import annotations

import unittest

from hypothesis import given, settings
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401
from scripts.run_python_tests import resolve_hypothesis_settings
from tests.finite_domain import (
    FINITE_DOMAIN_ATTRIBUTE,
    finite_generated_domain,
)
from tests.finite_domain_metadata import (
    FiniteDomainSpec,
)


class TestFiniteGeneratedDomain(unittest.TestCase):
    def test_decorator_runs_proof_and_fixes_the_exact_budget(self) -> None:
        proofs: list[str] = []

        def verify() -> None:
            proofs.append("verified")

        class Generated(unittest.TestCase):
            @finite_generated_domain(cardinality=4, verify=verify)
            @given(value=st.integers(min_value=0, max_value=3))
            def test_property(self, value: int) -> None:
                self.assertIn(value, range(4))

        case = Generated("test_property")
        resolved = resolve_hypothesis_settings(case)
        self.assertIsNotNone(resolved)
        assert resolved is not None
        self.assertEqual(resolved.configured.max_examples, 4)
        self.assertEqual(proofs, ["verified"])
        spec = getattr(Generated.test_property, FINITE_DOMAIN_ATTRIBUTE)
        self.assertEqual(spec, FiniteDomainSpec(cardinality=4, verify=verify))
        for field in (
            "backend",
            "database",
            "deadline",
            "derandomize",
            "phases",
            "print_blob",
            "report_multiple_bugs",
            "stateful_step_count",
            "suppress_health_check",
            "verbosity",
        ):
            self.assertEqual(
                getattr(resolved.configured, field),
                getattr(settings.default, field),
                field,
            )

    def test_nonpositive_cardinality_is_rejected_before_proof(self) -> None:
        proofs: list[str] = []

        with self.assertRaisesRegex(ValueError, "cardinality must be positive"):
            finite_generated_domain(
                cardinality=0,
                verify=lambda: proofs.append("should not run"),
            )

        self.assertEqual(proofs, [])

    def test_failed_domain_proof_aborts_decoration(self) -> None:
        def reject_collapsed_worlds() -> None:
            raise AssertionError("domain collapsed")

        decorator = finite_generated_domain(
            cardinality=2,
            verify=reject_collapsed_worlds,
        )

        with self.assertRaisesRegex(AssertionError, "domain collapsed"):
            decorator(lambda: None)

if __name__ == "__main__":
    unittest.main()
