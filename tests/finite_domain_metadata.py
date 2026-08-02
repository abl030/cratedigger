"""Wire-neutral metadata for explicitly proved finite generated domains."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field

FINITE_DOMAIN_ATTRIBUTE = "__cratedigger_finite_domain__"


@dataclass(frozen=True)
class FiniteDomainSpec:
    """Cardinality proved independently by a generated property's module."""

    cardinality: int
    verify: Callable[[], None] = field(repr=False, compare=False)


def certify_finite_domain(
    cardinality: int,
    verify: Callable[[], None],
) -> FiniteDomainSpec:
    """Run the independent proof and retain it for discovery to rerun."""
    verify()
    return FiniteDomainSpec(cardinality=cardinality, verify=verify)
