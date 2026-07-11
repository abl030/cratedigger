#!/usr/bin/env python3
"""Contract tests for web/routes/triage.py.

Split from tests/web/test_routes_pipeline.py (#481 item 3), which itself
split from tests/test_web_server.py (#408). Shared harness in
tests/web/_harness.py.
"""

from datetime import datetime, timezone
import os
import sys
import tempfile
import unittest
from unittest.mock import patch

import msgspec

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from tests.web._harness import _assert_required_fields, _FakeDbWebServerCase

from tests.helpers import make_request_row


class TestTriageRouteContracts(_FakeDbWebServerCase):
    """U17 contracts for ``GET /api/triage/<id>`` and ``GET /api/triage/list``.

    Both endpoints wrap ``lib.triage_service`` (U15) — the same service
    layer ``pipeline-cli triage show/list`` (U16) wraps. The wire shape
    on the cohort + composition payloads is the
    ``msgspec.to_builtins(TriageResult)`` shape verbatim, so the same
    Struct round-trips through ``msgspec.convert`` on both sides (CLI
    ⇄ API surface symmetry).

    Tests drive the real ``compose_triage_for_request`` and
    ``list_triage`` paths against the per-test :class:`FakePipelineDB`
    (``self.db``) — no service-layer mocking,
    per ``code-quality.md`` § MOCKS: LEAF-SEAM ONLY. Seeded rows use
    production-shape values: ``datetime.datetime`` for timestamps via
    ``make_request_row``'s defaults, real ``FieldResolutionRow`` /
    ``SearchLogRow`` via the typed seed helpers.
    """

    # The frontend triage drawer renders these top-level fields out of
    # ``msgspec.to_builtins(TriageResult)``. Pin every one so a future
    # field rename can't silently break the JS without flipping a test.
    SHOW_REQUIRED_FIELDS = {
        "request_meta", "unfindable", "field_quality", "search_forensics",
    }

    # ``request_meta`` fields the frontend depends on for the "Artist –
    # Album (year) #N" header + identity probes (failure_class, source,
    # search_filetype_override).
    SHOW_REQUEST_META_FIELDS = {
        "id", "artist_name", "album_title", "year", "status", "source",
        "mb_release_id", "discogs_release_id", "release_group_year",
        "is_va_compilation", "catalog_number", "failure_class",
        "search_filetype_override",
    }

    LIST_REQUIRED_FIELDS = {"results", "next_after", "page_size", "filter"}
    QUARANTINE_REQUIRED_FIELDS = {
        "quarantine_root", "folders", "special_buckets",
    }
    QUARANTINE_FOLDER_REQUIRED_FIELDS = {"name", "path", "mtime_ns"}

    def _get_quarantine(self, root: str):
        config_path = os.path.join(root, "config.ini")
        with open(config_path, "w", encoding="utf-8") as f:
            f.write(f"[Slskd]\ndownload_dir = {root}\n")
        with patch.dict(
            os.environ,
            {"CRATEDIGGER_RUNTIME_CONFIG": config_path},
            clear=False,
        ):
            return self._get("/api/triage/quarantine")

    def test_quarantine_returns_200_with_typed_required_fields(self):
        from lib.quarantine_triage_service import QuarantineTriageResult

        with tempfile.TemporaryDirectory() as root:
            quarantine = os.path.join(root, "failed_imports")
            referenced = os.path.join(quarantine, "Referenced")
            orphan = os.path.join(quarantine, "Orphan")
            os.makedirs(referenced)
            os.makedirs(orphan)
            request_id = self.db.add_request("Artist", "Album", "request")
            self.db.log_download(
                request_id,
                outcome="rejected",
                validation_result={
                    "failed_path": "failed_imports/Referenced",
                    "scenario": "high_distance",
                },
            )

            status, data = self._get_quarantine(root)

        self.assertEqual(status, 200)
        _assert_required_fields(
            self, data, self.QUARANTINE_REQUIRED_FIELDS,
            "quarantine triage response",
        )
        self.assertEqual(len(data["folders"]), 1)
        _assert_required_fields(
            self, data["folders"][0], self.QUARANTINE_FOLDER_REQUIRED_FIELDS,
            "quarantine folder",
        )
        result = msgspec.convert(data, type=QuarantineTriageResult)
        self.assertEqual(result.folders[0].name, "Orphan")

    def test_quarantine_filesystem_failure_returns_503(self):
        with tempfile.TemporaryDirectory() as root:
            with open(os.path.join(root, "failed_imports"), "w", encoding="utf-8") as f:
                f.write("not a directory")
            status, data = self._get_quarantine(root)

        self.assertEqual(status, 503)
        self.assertIn("error", data)

    # --- /api/triage/<id> -------------------------------------------------

    def test_show_returns_200_with_required_fields_and_roundtrips(self):
        """Happy path: a seeded request composes through to a 200 with
        the full TriageResult shape, and the response body round-trips
        through ``msgspec.convert(payload, type=TriageResult)`` — the
        wire-boundary contract per CLI ⇄ API symmetry."""
        from lib.triage_service import TriageResult
        self.db.seed_request(make_request_row(
            id=4242,
            artist_name="Triage Artist",
            album_title="Triage Album",
            status="wanted",
            failure_class="search_not_converting",
            unfindable_category="artist_absent",
            unfindable_categorised_at=datetime(2026, 5, 20, tzinfo=timezone.utc),
            last_artist_probe_at=datetime(2026, 5, 22, tzinfo=timezone.utc),
            last_artist_probe_match_count=0,
        ))

        status, data = self._get("/api/triage/4242")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.SHOW_REQUIRED_FIELDS,
                                "triage show response")
        _assert_required_fields(self, data["request_meta"],
                                self.SHOW_REQUEST_META_FIELDS,
                                "triage show request_meta")
        # The wire shape is exactly the Struct shape — round-trip proves
        # no field drift / coercion happened at the boundary.
        composed = msgspec.convert(data, type=TriageResult)
        self.assertEqual(composed.request_meta.id, 4242)
        self.assertEqual(composed.request_meta.artist_name, "Triage Artist")
        self.assertEqual(
            composed.request_meta.failure_class, "search_not_converting",
        )
        # Unfindable struct populated because the seeded row has signals.
        self.assertIsNotNone(composed.unfindable)
        assert composed.unfindable is not None
        self.assertEqual(composed.unfindable.category, "artist_absent")

    def test_show_returns_404_when_request_id_missing(self):
        """Unknown request id → 404 with ``error`` + ``request_id`` in body
        so the frontend can surface "not found" with the right id."""
        status, data = self._get("/api/triage/99999")
        self.assertEqual(status, 404)
        self.assertIn("error", data)
        self.assertEqual(data["request_id"], 99999)

    def test_show_returns_404_for_non_int_path(self):
        """A non-numeric path segment doesn't even match the regex
        (which requires ``\\d+``), so the route table itself replies
        404. This test pins the route-table contract (no silent
        coercion to a different handler)."""
        status, _ = self._get("/api/triage/not-an-int")
        # The regex r"^/api/triage/(\d+)$" does not match — falls
        # through to the catch-all 404.
        self.assertEqual(status, 404)

    # --- /api/triage/list --------------------------------------------------

    def test_list_filter_unfindable_returns_200_with_required_fields(self):
        """A seeded unfindable request shows up under
        ``filter=unfindable`` with the documented envelope shape."""
        from lib.triage_service import TriageResult
        self.db.seed_request(make_request_row(
            id=10, artist_name="Stuck Artist",
            unfindable_category="artist_absent",
            unfindable_categorised_at=datetime(2026, 5, 20, tzinfo=timezone.utc),
        ))
        # Decoy row without any unfindable signal — must NOT appear in
        # the filtered cohort.
        self.db.seed_request(make_request_row(
            id=11, artist_name="Healthy Artist", status="imported",
        ))

        status, data = self._get("/api/triage/list?filter=unfindable")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.LIST_REQUIRED_FIELDS,
                                "triage list response")
        self.assertEqual(data["filter"], "unfindable")
        self.assertEqual(data["page_size"], 50)
        # Only the unfindable row should be returned.
        self.assertEqual(len(data["results"]), 1)
        composed = msgspec.convert(data["results"][0], type=TriageResult)
        self.assertEqual(composed.request_meta.id, 10)
        # Page is shorter than page_size → next_after is None
        # (cohort exhausted).
        self.assertIsNone(data["next_after"])

    def test_list_filter_data_quality_status_filters_by_status_column(self):
        """``filter=data_quality:status=<status>`` (issue #374 canonical
        form) returns only requests with at least one
        ``album_request_field_resolutions`` row whose ``status`` column
        matches the spec. Mirrors what
        ``lib/field_resolver_service.py::_classify_lookup_exception``
        actually writes."""
        # Seeded request A: has a release_group_year resolution in the
        # sticky 4xx-client bucket — matches.
        self.db.seed_request(make_request_row(id=20))
        self.db.record_field_resolution(
            request_id=20, field_name="release_group_year",
            status="unresolved_4xx_client", reason_code="http_400",
        )
        # Seeded request B: has a field resolution but a different
        # status bucket — must NOT appear.
        self.db.seed_request(make_request_row(id=21))
        self.db.record_field_resolution(
            request_id=21, field_name="catalog_number",
            status="unresolved_mirror_unavailable",
            reason_code="ConnectionError",
        )

        status, data = self._get(
            "/api/triage/list?filter=data_quality:status=unresolved_4xx_client"
        )

        self.assertEqual(status, 200)
        self.assertEqual(
            data["filter"], "data_quality:status=unresolved_4xx_client",
        )
        self.assertEqual(len(data["results"]), 1)
        self.assertEqual(data["results"][0]["request_meta"]["id"], 20)

    def test_list_filter_data_quality_reason_filters_by_reason_code(self):
        """``filter=data_quality:reason=<code>`` filters on the
        ``reason_code`` column (HTTP code specifier — http_400,
        http_410, http_422, etc.)."""
        # Seeded request A: 4xx-client status, reason_code=http_400 — matches.
        self.db.seed_request(make_request_row(id=22))
        self.db.record_field_resolution(
            request_id=22, field_name="release_group_year",
            status="unresolved_4xx_client", reason_code="http_400",
        )
        # Seeded request B: 4xx-client status but reason_code=http_410 — excluded.
        self.db.seed_request(make_request_row(id=23))
        self.db.record_field_resolution(
            request_id=23, field_name="catalog_number",
            status="unresolved_4xx_client", reason_code="http_410",
        )

        status, data = self._get(
            "/api/triage/list?filter=data_quality:reason=http_400"
        )

        self.assertEqual(status, 200)
        self.assertEqual(data["filter"], "data_quality:reason=http_400")
        self.assertEqual(len(data["results"]), 1)
        self.assertEqual(data["results"][0]["request_meta"]["id"], 22)

    def test_list_invalid_filter_returns_400_with_valid_filters_array(self):
        """An unparseable filter spec surfaces as a 400 carrying
        ``error`` + a ``valid_filters`` array, so the operator can
        self-correct without leaving the network response."""
        status, data = self._get("/api/triage/list?filter=garbage_value")

        self.assertEqual(status, 400)
        self.assertIn("error", data)
        self.assertIn("valid_filters", data)
        self.assertIsInstance(data["valid_filters"], list)
        # The four canonical scalar forms must be advertised.
        self.assertIn("all", data["valid_filters"])
        self.assertIn("unfindable", data["valid_filters"])
        self.assertIn("data_quality", data["valid_filters"])
        self.assertIn("search_not_converting", data["valid_filters"])

    def test_list_limit_caps_results_and_emits_next_after_cursor(self):
        """When the page is exactly ``limit`` long the response carries
        ``next_after`` = last request_id so the operator can paginate."""
        for rid in (30, 31, 32):
            self.db.seed_request(make_request_row(
                id=rid, status="imported",
            ))

        status, data = self._get("/api/triage/list?filter=all&limit=2")

        self.assertEqual(status, 200)
        self.assertEqual(data["page_size"], 2)
        self.assertEqual(len(data["results"]), 2)
        # Page was full → next_after is the last id in the page.
        self.assertEqual(data["next_after"],
                         data["results"][-1]["request_meta"]["id"])

    def test_list_default_filter_when_query_string_omitted(self):
        """Missing ``filter=`` defaults to ``all`` so a bare hit on
        ``/api/triage/list`` is meaningful."""
        self.db.seed_request(make_request_row(id=40, status="imported"))

        status, data = self._get("/api/triage/list")

        self.assertEqual(status, 200)
        self.assertEqual(data["filter"], "all")
        self.assertGreaterEqual(len(data["results"]), 1)

    def test_list_rejects_non_int_limit(self):
        status, data = self._get("/api/triage/list?filter=all&limit=abc")
        self.assertEqual(status, 400)
        self.assertIn("error", data)

    def test_list_rejects_out_of_bounds_limit(self):
        status, data = self._get("/api/triage/list?filter=all&limit=500")
        self.assertEqual(status, 400)
        self.assertIn("error", data)

    def test_list_rejects_non_int_after(self):
        status, data = self._get(
            "/api/triage/list?filter=all&after=not-an-int")
        self.assertEqual(status, 400)
        self.assertIn("error", data)


if __name__ == "__main__":
    unittest.main()
