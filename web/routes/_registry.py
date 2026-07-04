"""Declarative route registry (#496).

Every ``web/routes/*.py`` module used to maintain up to five parallel
structures for the same route set: ``GET_ROUTES``/``POST_ROUTES``/
``GET_PATTERNS`` dispatch tables, ``GET_DESCRIPTIONS``/
``POST_DESCRIPTIONS``/``PATTERN_DESCRIPTIONS`` dicts (merged by hand in
``web/server.py::Handler``), and a hand-maintained
``TestRouteContractAudit.CLASSIFIED_ROUTES`` set in
``tests/web/test_route_audit.py``. Adding a route meant touching three
files and up to six literals — and the audits existed precisely because
that invited drift.

This module collapses all of it into ONE :class:`RouteRegistration` per
route — path/pattern, method, handler, description, and contract
classification in a single declaration next to the handler. Each route
module exports one ``ROUTES: list[RouteRegistration]``; ``web/server.py``
merges those lists and derives its dispatch tables from the merge;
``/api/_index`` and the route-classification audit introspect the merged
list directly instead of separately-maintained dicts/sets.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Callable, Literal


@dataclass(frozen=True)
class RouteRegistration:
    """One route's full contract in a single declaration.

    Static routes: ``pattern`` is ``None`` and ``path`` is the literal
    URL. Pattern routes: ``pattern`` is the compiled regex and ``path``
    is ``pattern.pattern`` — kept as a plain string so callers that only
    care about the string key (the classification audit, ``/api/_index``)
    don't need to branch on which kind of route this is.

    Use the :func:`route` / :func:`pattern_route` constructors rather
    than instantiating this directly — they keep ``path`` and ``pattern``
    in sync and make ``classified`` an explicit, mandatory choice at
    every call site.
    """

    method: Literal["GET", "POST"]
    path: str
    handler: Callable[..., None]
    description: str
    classified: bool = False
    pattern: re.Pattern[str] | None = None


def route(
    method: Literal["GET", "POST"],
    path: str,
    handler: Callable[..., None],
    description: str,
    *,
    classified: bool,
) -> RouteRegistration:
    """Register a static-path route (no URL captures)."""
    return RouteRegistration(
        method=method,
        path=path,
        handler=handler,
        description=description,
        classified=classified,
        pattern=None,
    )


def pattern_route(
    method: Literal["GET", "POST"],
    pattern: str,
    handler: Callable[..., None],
    description: str,
    *,
    classified: bool,
) -> RouteRegistration:
    """Register a regex-pattern route. Compiles ``pattern`` once here so
    call sites never have to write the regex string twice."""
    compiled = re.compile(pattern)
    return RouteRegistration(
        method=method,
        path=compiled.pattern,
        handler=handler,
        description=description,
        classified=classified,
        pattern=compiled,
    )


def merge_registries(*modules: object) -> list[RouteRegistration]:
    """Flatten each route module's ``ROUTES`` list into one ordered list.

    ``modules`` are the imported ``web.routes.*`` modules themselves
    (each exposing a module-level ``ROUTES: list[RouteRegistration]``).
    """
    merged: list[RouteRegistration] = []
    for mod in modules:
        merged.extend(mod.ROUTES)  # type: ignore[attr-defined]
    return merged


def build_get_routes(routes: list[RouteRegistration]) -> dict[str, object]:
    return {
        r.path: r.handler for r in routes
        if r.method == "GET" and r.pattern is None
    }


def build_get_patterns(
    routes: list[RouteRegistration],
) -> list[tuple[re.Pattern[str], object]]:
    return [
        (r.pattern, r.handler) for r in routes
        if r.method == "GET" and r.pattern is not None
    ]


def build_post_routes(routes: list[RouteRegistration]) -> dict[str, object]:
    return {
        r.path: r.handler for r in routes
        if r.method == "POST" and r.pattern is None
    }


def build_post_patterns(
    routes: list[RouteRegistration],
) -> list[tuple[re.Pattern[str], object]]:
    return [
        (r.pattern, r.handler) for r in routes
        if r.method == "POST" and r.pattern is not None
    ]


def unclassified_routes(routes: list[RouteRegistration]) -> list[str]:
    """Keys of every route registration missing ``classified=True``.

    This is the audit's core logic — ``TestRouteContractAudit`` calls it
    against the live merged registry; the RED test in
    ``tests/web/test_route_audit.py`` calls it against a synthetic list
    to prove the detector fires independent of the live route set.
    """
    return sorted(r.path for r in routes if not r.classified)


def missing_or_empty_descriptions(routes: list[RouteRegistration]) -> list[str]:
    """Keys of every route registration with a missing/blank description."""
    return sorted(r.path for r in routes if not (r.description and r.description.strip()))
