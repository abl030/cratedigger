#!/usr/bin/env python3
"""Route classification audit + /api/_index discoverability contracts.

Split from tests/test_web_server.py (#408). Shared harness in
tests/web/_harness.py.
"""

import os
import sys
import unittest


sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from tests.web._harness import _assert_required_fields, _WebServerCase


class TestRouteRegistryClassificationAudit(unittest.TestCase):
    """#496 — the classification audit reads the ``classified`` field on
    each route module's ``RouteRegistration`` list instead of a
    hand-maintained ``CLASSIFIED_ROUTES`` set. This proves the detector
    still has teeth: a registration that omits ``classified=True`` is
    flagged, by name, independent of the live server's route set.
    """

    def test_unclassified_routes_helper_flags_a_bogus_route(self):
        from web.routes._registry import route, unclassified_routes

        bogus = route(
            "GET", "/api/_bogus_496_test_route",
            lambda h, params: None,
            "Deliberately unclassified bogus route for the RED test.",
            classified=False,
        )
        classified_sibling = route(
            "GET", "/api/_bogus_496_test_route_classified",
            lambda h, params: None,
            "Deliberately classified sibling so the helper isn't vacuous.",
            classified=True,
        )
        self.assertEqual(
            unclassified_routes([bogus, classified_sibling]),
            ["/api/_bogus_496_test_route"],
        )


class TestRouteContractAudit(unittest.TestCase):
    """Every web/routes.py endpoint must be covered by a frontend contract
    decision.

    #496: there is no hand-maintained ``CLASSIFIED_ROUTES`` set anymore —
    classification lives on each route's own ``RouteRegistration`` (see
    ``web/routes/_registry.py``), declared next to its handler. A route
    that is registered without ``classified=True`` fails here, by name;
    there is no separate "stale entry" case to test because the
    classification is deleted along with the route declaration itself
    (they're the same line), not tracked in a second list that can drift.
    """

    def test_all_web_routes_are_classified_for_contract_coverage(self):
        import web.server as srv
        from web.routes._registry import unclassified_routes

        unclassified = unclassified_routes(srv.ALL_ROUTES)
        self.assertFalse(
            unclassified, f"Unclassified web routes: {unclassified}")

    def test_every_registered_route_has_a_description(self):
        """Every registered route must carry a human-readable one-liner
        on its ``RouteRegistration``. Fails if a future route is added
        without one — fixing it is a one-line edit in the route module."""
        import web.server as srv
        from web.routes._registry import missing_or_empty_descriptions

        missing = missing_or_empty_descriptions(srv.ALL_ROUTES)
        self.assertFalse(
            missing,
            f"Routes with a missing/empty description: {missing}",
        )


class TestRouteRegistryShape(unittest.TestCase):
    """Structural test that the merged registry has the expected shape —
    successor to the old U18-step-1 description-dispatch-table check now
    that descriptions live on ``RouteRegistration`` (#496)."""

    def test_all_routes_are_well_formed_registrations(self):
        import re
        import web.server as srv
        from web.routes._registry import RouteRegistration

        self.assertIsInstance(srv.ALL_ROUTES, list)
        self.assertGreaterEqual(len(srv.ALL_ROUTES), 30,
                                f"only {len(srv.ALL_ROUTES)} routes registered")
        for r in srv.ALL_ROUTES:
            self.assertIsInstance(r, RouteRegistration)
            self.assertIn(r.method, ("GET", "POST"))
            self.assertIsInstance(r.path, str)
            self.assertTrue(callable(r.handler))
            self.assertIsInstance(r.description, str)
            self.assertIsInstance(r.classified, bool)
            if r.pattern is not None:
                self.assertIsInstance(r.pattern, re.Pattern)
                self.assertEqual(r.pattern.pattern, r.path)


class TestApiIndexRouteContract(_WebServerCase):
    """U18 step 2: contract test for the self-documenting ``/api/_index``."""

    INDEX_ENTRY_REQUIRED_FIELDS = {
        "method", "path", "description", "request_model",
    }

    def test_api_index_returns_classified_routes_with_pydantic_models(self):
        status, data = self._get("/api/_index")
        self.assertEqual(status, 200)
        self.assertIsInstance(data, list)
        # We register ~40+ routes; assert a healthy floor so a regression
        # that empties the merge can't silently sneak through.
        self.assertGreaterEqual(len(data), 30, msg=f"only {len(data)} entries")

        for entry in data:
            _assert_required_fields(
                self, entry, self.INDEX_ENTRY_REQUIRED_FIELDS,
                f"_index entry {entry.get('path')!r}",
            )
            self.assertIn(entry["method"], {"GET", "POST"})
            self.assertIsInstance(entry["path"], str)
            self.assertIsInstance(entry["description"], str)

        # The Pydantic introspection must surface at least one known
        # POST handler so we know the regex is biting the real source.
        post_models = {
            (e["path"], e["request_model"])
            for e in data if e["method"] == "POST"
        }
        self.assertIn(
            ("/api/pipeline/add", "PipelineAddRequest"),
            post_models,
            f"PipelineAddRequest not surfaced in post_models: {post_models}",
        )

        # Sort invariant — operators consume this as a stable index.
        sorted_entries = sorted(
            data, key=lambda e: (str(e["method"]), str(e["path"])))
        self.assertEqual(data, sorted_entries)

if __name__ == "__main__":
    unittest.main()
