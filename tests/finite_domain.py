"""Explicit contracts for generated properties with proved finite domains."""

from __future__ import annotations

from collections.abc import Callable
from typing import ParamSpec, TypeVar

from hypothesis import settings

import tests._hypothesis_profiles  # noqa: F401 - registers suite/fuzz tiers
from tests.finite_domain_metadata import (
    FINITE_DOMAIN_ATTRIBUTE,
    certify_finite_domain,
)

_Args = ParamSpec("_Args")
_Result = TypeVar("_Result")


def finite_generated_domain(
    *,
    cardinality: int,
    verify: Callable[[], None],
) -> Callable[
    [Callable[_Args, _Result]],
    Callable[_Args, _Result],
]:
    """Prove a finite domain, fix its exact budget, and expose runner metadata.

    ``verify`` must independently enumerate or otherwise prove the semantic
    worlds represented by the strategy.  It runs while the test module is
    imported, including isolated fuzz discovery; a broken proof therefore
    fails before target admission.  ``settings(max_examples=...)`` inherits
    every unnamed setting from the already-loaded suite/fuzz profile.
    """
    if cardinality < 1:
        raise ValueError("finite generated-domain cardinality must be positive")
    def decorate(
        test: Callable[_Args, _Result],
    ) -> Callable[_Args, _Result]:
        spec = certify_finite_domain(cardinality, verify)
        configured = settings(max_examples=cardinality)(test)
        setattr(configured, FINITE_DOMAIN_ATTRIBUTE, spec)
        return configured

    return decorate
