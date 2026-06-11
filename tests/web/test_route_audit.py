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



class TestRouteContractAudit(unittest.TestCase):
    """Every web/routes.py endpoint must be covered by a frontend contract decision."""

    CLASSIFIED_ROUTES = {
        # U18 step 2: self-documenting API surface.
        "/api/_index",
        "/api/search",
        "/api/browse/resolve",
        "/api/library/artist",
        "/api/artist/compare",
        r"^/api/artist/([a-f0-9-]+)$",
        r"^/api/artist/([a-f0-9-]+)/disambiguate$",
        r"^/api/release-group/([a-f0-9-]+)$",
        r"^/api/release/([a-f0-9-]+)$",
        "/api/discogs/search",
        r"^/api/discogs/artist/(\d+)$",
        r"^/api/discogs/master/(\d+)$",
        r"^/api/discogs/release/(\d+)$",
        "/api/discogs/label/search",
        r"^/api/discogs/label/(\d+)$",
        "/api/disk-coverage",
        "/api/pipeline/log",
        "/api/pipeline/status",
        "/api/pipeline/recent",
        "/api/pipeline/all",
        "/api/pipeline/downloading",
        "/api/pipeline/dashboard",
        "/api/pipeline/constants",
        "/api/pipeline/simulate",
        r"^/api/pipeline/(\d+)$",
        r"^/api/pipeline/(\d+)/search-plan$",
        r"^/api/pipeline/(\d+)/search-plan/dry-run$",
        r"^/api/pipeline/(\d+)/search-plan/saturation$",
        r"^/api/pipeline/(\d+)/search-plan/history$",
        r"^/api/pipeline/(\d+)/search-plan/regenerate$",
        r"^/api/pipeline/(\d+)/search-plan/advance$",
        r"^/api/pipeline/(\d+)/replace$",
        r"^/api/pipeline/(\d+)/resolve-rg$",
        r"^/api/pipeline/requests-by-rg/([a-f0-9-]{36})$",
        r"^/api/beets-distance/(\d+)/([a-f0-9-]{36})$",
        "/api/pipeline/active-rgs",
        # U1: Long-Tail Triage Console worklist read. Wraps
        # ``lib.long_tail_service.list_long_tail`` — same service as
        # ``pipeline-cli long-tail`` per CLI ⇄ API symmetry.
        "/api/pipeline/long-tail",
        "/api/pipeline/add",
        "/api/pipeline/update",
        "/api/pipeline/upgrade",
        "/api/pipeline/set-quality",
        "/api/pipeline/set-intent",
        "/api/pipeline/ban-source",
        "/api/pipeline/force-import",
        "/api/pipeline/delete",
        "/api/import-jobs",
        "/api/import-jobs/timeline",
        r"^/api/import-jobs/(\d+)$",
        "/api/beets/search",
        "/api/beets/recent",
        r"^/api/beets/album/(\d+)$",
        "/api/beets/delete",
        "/api/manual-import/scan",
        "/api/manual-import/import",
        "/api/import-preview",
        "/api/wrong-matches",
        "/api/wrong-matches/audio",
        "/api/wrong-matches/delete",
        "/api/wrong-matches/delete-group",
        "/api/wrong-matches/converge",
        "/api/wrong-matches/triage",
        "/api/wrong-matches/triage/status",
        "/api/wrong-matches/explorer",
        # U17: /api/triage HTTP endpoints. Per-request composition and
        # cohort listing both wrap ``lib.triage_service`` (U15) — same
        # service as ``pipeline-cli triage`` (U16) per CLI ⇄ API symmetry.
        "/api/triage/list",
        r"^/api/triage/(\d+)$",
        # U8: YouTube Music album resolver. Wraps
        # ``lib.youtube_album_service.resolve_youtube_album`` — same
        # service as ``pipeline-cli youtube-album`` (U7) per
        # CLI ⇄ API symmetry. Outcome → HTTP status from
        # ``OUTCOME_HTTP_STATUS`` (single source of truth).
        "/api/youtube-album",
        # U5: YouTube rescue ingest submit. Wraps
        # ``lib.youtube_ingest_service.YoutubeIngestService.submit`` —
        # same service as ``pipeline-cli youtube-rescue`` (U4) per
        # CLI ⇄ API symmetry. Outcome → HTTP status from
        # ``OUTCOME_HTTP_STATUS`` on the ingest service.
        r"^/api/pipeline/(\d+)/youtube-rescue$",
    }

    def test_all_web_routes_are_classified_for_contract_coverage(self):
        import web.server as srv

        actual = set(srv.Handler._FUNC_GET_ROUTES)
        actual.update(srv.Handler._FUNC_POST_ROUTES)
        actual.update(pattern.pattern for pattern, _fn in srv.Handler._FUNC_GET_PATTERNS)
        actual.update(
            pattern.pattern for pattern, _fn
            in getattr(srv.Handler, "_FUNC_POST_PATTERNS", []))

        self.assertFalse(actual - self.CLASSIFIED_ROUTES,
                         f"Unclassified web routes: {sorted(actual - self.CLASSIFIED_ROUTES)}")
        self.assertFalse(self.CLASSIFIED_ROUTES - actual,
                         f"Stale route classifications: {sorted(self.CLASSIFIED_ROUTES - actual)}")

    def test_every_registered_route_has_a_description(self):
        """U18 step 3: every registered route must carry a human-readable
        one-liner in the parallel description dispatch tables. Fails if a
        future route is added without one — fixing it is a one-line edit
        in the route module."""
        import web.server as srv

        get_paths = set(srv.Handler._FUNC_GET_ROUTES.keys())
        post_paths = set(srv.Handler._FUNC_POST_ROUTES.keys())
        get_pattern_strs = {
            p.pattern for p, _fn in srv.Handler._FUNC_GET_PATTERNS}
        post_pattern_strs = {
            p.pattern for p, _fn in srv.Handler._FUNC_POST_PATTERNS}

        get_desc_paths = set(srv.Handler._FUNC_GET_DESCRIPTIONS.keys())
        post_desc_paths = set(srv.Handler._FUNC_POST_DESCRIPTIONS.keys())
        get_pattern_desc_strs = {
            p.pattern for p, _d in srv.Handler._FUNC_GET_PATTERN_DESCRIPTIONS}
        post_pattern_desc_strs = {
            p.pattern for p, _d in srv.Handler._FUNC_POST_PATTERN_DESCRIPTIONS}

        missing_get = get_paths - get_desc_paths
        missing_post = post_paths - post_desc_paths
        missing_get_patterns = get_pattern_strs - get_pattern_desc_strs
        missing_post_patterns = post_pattern_strs - post_pattern_desc_strs

        self.assertFalse(
            missing_get,
            f"GET routes missing descriptions: {sorted(missing_get)}",
        )
        self.assertFalse(
            missing_post,
            f"POST routes missing descriptions: {sorted(missing_post)}",
        )
        self.assertFalse(
            missing_get_patterns,
            "GET pattern routes missing descriptions: "
            f"{sorted(missing_get_patterns)}",
        )
        self.assertFalse(
            missing_post_patterns,
            "POST pattern routes missing descriptions: "
            f"{sorted(missing_post_patterns)}",
        )

        # Empty-string registration would pass the presence check above
        # and defeat the U18 intent — every route must carry a non-empty
        # one-liner. Surface each offender by name so the fix is
        # one-route-at-a-time.
        def _empty_desc_paths(registered: dict[str, str]) -> list[str]:
            return sorted(p for p, d in registered.items() if not (d and d.strip()))

        empty_get = _empty_desc_paths(srv.Handler._FUNC_GET_DESCRIPTIONS)
        empty_post = _empty_desc_paths(srv.Handler._FUNC_POST_DESCRIPTIONS)
        empty_get_pat = sorted(
            p.pattern
            for p, d in srv.Handler._FUNC_GET_PATTERN_DESCRIPTIONS
            if not (d and d.strip())
        )
        empty_post_pat = sorted(
            p.pattern
            for p, d in srv.Handler._FUNC_POST_PATTERN_DESCRIPTIONS
            if not (d and d.strip())
        )
        self.assertFalse(
            empty_get,
            f"GET routes with empty description string: {empty_get}",
        )
        self.assertFalse(
            empty_post,
            f"POST routes with empty description string: {empty_post}",
        )
        self.assertFalse(
            empty_get_pat,
            f"GET pattern routes with empty description string: {empty_get_pat}",
        )
        self.assertFalse(
            empty_post_pat,
            f"POST pattern routes with empty description string: {empty_post_pat}",
        )


class TestRouteDescriptionMechanism(unittest.TestCase):
    """U18 step 1: structural test that the route-description dispatch tables exist.

    Proves the registration plumbing mirrors the GET_ROUTES / POST_ROUTES /
    GET_PATTERNS / POST_PATTERNS pattern in web/server.py. Contents are
    populated in U18 step 2; empty is fine here.
    """

    def test_description_dispatch_tables_exist_with_correct_shapes(self):
        import re
        import web.server as srv

        # All four class attributes must exist.
        self.assertTrue(hasattr(srv.Handler, "_FUNC_GET_DESCRIPTIONS"))
        self.assertTrue(hasattr(srv.Handler, "_FUNC_POST_DESCRIPTIONS"))
        self.assertTrue(hasattr(srv.Handler, "_FUNC_GET_PATTERN_DESCRIPTIONS"))
        self.assertTrue(hasattr(srv.Handler, "_FUNC_POST_PATTERN_DESCRIPTIONS"))

        get_desc = srv.Handler._FUNC_GET_DESCRIPTIONS
        post_desc = srv.Handler._FUNC_POST_DESCRIPTIONS
        get_pattern_desc = srv.Handler._FUNC_GET_PATTERN_DESCRIPTIONS
        post_pattern_desc = srv.Handler._FUNC_POST_PATTERN_DESCRIPTIONS

        # Dict shapes: path (str) → description (str).
        self.assertIsInstance(get_desc, dict)
        self.assertIsInstance(post_desc, dict)
        for path, desc in get_desc.items():
            self.assertIsInstance(path, str)
            self.assertIsInstance(desc, str)
        for path, desc in post_desc.items():
            self.assertIsInstance(path, str)
            self.assertIsInstance(desc, str)

        # List-of-tuple shapes: (re.Pattern, str).
        self.assertIsInstance(get_pattern_desc, list)
        self.assertIsInstance(post_pattern_desc, list)
        for entry in get_pattern_desc:
            self.assertIsInstance(entry, tuple)
            self.assertEqual(len(entry), 2)
            self.assertIsInstance(entry[0], re.Pattern)
            self.assertIsInstance(entry[1], str)
        for entry in post_pattern_desc:
            self.assertIsInstance(entry, tuple)
            self.assertEqual(len(entry), 2)
            self.assertIsInstance(entry[0], re.Pattern)
            self.assertIsInstance(entry[1], str)


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
