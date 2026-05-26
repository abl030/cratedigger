"""Tests for ``lib.triage_service`` — service layer for the operator
triage surface (U15).

Covers the three deliverables of the service:

* ``parse_filter`` — DSL for cohort selection. Garbage raises
  ``InvalidFilterError``; the wrappers map onto exit/status codes.
* ``compose_triage_for_request`` — single-request payload composed
  from ``album_requests`` + the three side domains.
* ``list_triage`` — cohort listing with N+1 mitigation. The contract
  is "4 queries (+ 1 headroom for future growth)" — the guard runs
  against ``FakePipelineDB`` which records per-method call counts.
"""

from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from lib.triage_service import (
    InvalidFilterError,
    ParsedTriageFilter,
    SearchForensicsSummary,
    SearchLogEntry,
    TriageResult,
    compose_triage_for_request,
    list_triage,
    parse_filter,
)
from tests.fakes import FakePipelineDB
from tests.helpers import make_request_row


# ---------------------------------------------------------------------------
# Filter parsing
# ---------------------------------------------------------------------------


class TestParseFilter(unittest.TestCase):
    """``parse_filter`` covers seven accepted shapes plus the garbage path."""

    def test_all(self) -> None:
        parsed = parse_filter("all")
        self.assertEqual(parsed.kind, "all")

    def test_unfindable_bare(self) -> None:
        parsed = parse_filter("unfindable")
        self.assertEqual(parsed.kind, "unfindable")
        self.assertIsNone(parsed.unfindable_category)

    def test_unfindable_with_category(self) -> None:
        parsed = parse_filter("unfindable:artist_absent")
        self.assertEqual(parsed.kind, "unfindable")
        self.assertEqual(parsed.unfindable_category, "artist_absent")

    def test_data_quality_bare(self) -> None:
        parsed = parse_filter("data_quality")
        self.assertEqual(parsed.kind, "data_quality")
        self.assertIsNone(parsed.field_name)
        self.assertIsNone(parsed.reason_code)

    def test_data_quality_with_field(self) -> None:
        parsed = parse_filter("data_quality:release_group_year")
        self.assertEqual(parsed.kind, "data_quality")
        self.assertEqual(parsed.field_name, "release_group_year")
        self.assertIsNone(parsed.reason_code)

    def test_data_quality_with_status(self) -> None:
        """#374 canonical form — sticky 4xx-client cohort uses
        ``status=<resolver_status>`` (matches the ``status`` column
        ``lib/field_resolver_service.py`` writes)."""
        parsed = parse_filter("data_quality:status=unresolved_4xx_client")
        self.assertEqual(parsed.kind, "data_quality")
        self.assertIsNone(parsed.field_name)
        self.assertIsNone(parsed.reason_code)
        self.assertEqual(parsed.status_code, "unresolved_4xx_client")

    def test_data_quality_with_reason_code(self) -> None:
        """Complementary form — filter on the specific HTTP code
        in the ``reason_code`` column (e.g. http_400, http_410)."""
        parsed = parse_filter("data_quality:reason=http_400")
        self.assertEqual(parsed.kind, "data_quality")
        self.assertIsNone(parsed.field_name)
        self.assertIsNone(parsed.status_code)
        self.assertEqual(parsed.reason_code, "http_400")

    def test_data_quality_unknown_field_raises(self) -> None:
        """A typo in the field name (e.g. ``release_year``) is rejected
        at parse time; the operator sees the four valid names in the
        error message."""
        with self.assertRaises(InvalidFilterError):
            parse_filter("data_quality:release_year")

    def test_search_not_converting(self) -> None:
        parsed = parse_filter("search_not_converting")
        self.assertEqual(parsed.kind, "search_not_converting")

    def test_trims_and_lowercases(self) -> None:
        parsed = parse_filter("  UNFINDABLE:Artist_Absent  ")
        self.assertEqual(parsed.kind, "unfindable")
        self.assertEqual(parsed.unfindable_category, "artist_absent")

    def test_empty_string_raises(self) -> None:
        with self.assertRaises(InvalidFilterError):
            parse_filter("")

    def test_unknown_kind_raises(self) -> None:
        with self.assertRaises(InvalidFilterError):
            parse_filter("not_a_filter")

    def test_unknown_unfindable_category_raises(self) -> None:
        with self.assertRaises(InvalidFilterError):
            parse_filter("unfindable:bogus_category")

    def test_data_quality_empty_suffix_raises(self) -> None:
        with self.assertRaises(InvalidFilterError):
            parse_filter("data_quality:")

    def test_data_quality_empty_reason_raises(self) -> None:
        with self.assertRaises(InvalidFilterError):
            parse_filter("data_quality:reason=")

    def test_data_quality_empty_status_raises(self) -> None:
        with self.assertRaises(InvalidFilterError):
            parse_filter("data_quality:status=")


# ---------------------------------------------------------------------------
# compose_triage_for_request
# ---------------------------------------------------------------------------


def _now() -> datetime:
    return datetime(2026, 5, 26, 12, 0, 0, tzinfo=timezone.utc)


class TestComposeTriage(unittest.TestCase):
    """One-request triage payload composition."""

    def test_non_existent_id_returns_none(self) -> None:
        db = FakePipelineDB()
        self.assertIsNone(compose_triage_for_request(9999, db))

    def test_healthy_request_no_unfindable_no_field_quality(self) -> None:
        """A converted/healthy request has unfindable=None and an empty
        field_quality list."""
        db = FakePipelineDB()
        row = make_request_row(
            id=1,
            artist_name="Healthy Artist",
            album_title="Imported Album",
            status="imported",
            failure_class="resolved",
        )
        db.seed_request(row)

        result = compose_triage_for_request(1, db)

        assert result is not None
        self.assertEqual(result.request_meta.id, 1)
        self.assertEqual(result.request_meta.artist_name, "Healthy Artist")
        self.assertEqual(result.request_meta.status, "imported")
        self.assertEqual(result.request_meta.failure_class, "resolved")
        self.assertIsNone(result.unfindable)
        self.assertEqual(result.field_quality, [])
        # Empty search forensics: zero counters, empty recent_entries.
        self.assertEqual(result.search_forensics.total_searches, 0)
        self.assertEqual(result.search_forensics.recent_entries, [])

    def test_unfindable_populated_when_any_signal_present(self) -> None:
        db = FakePipelineDB()
        categorised_at = _now() - timedelta(days=3)
        row = make_request_row(
            id=42,
            artist_name="Unfindable Artist",
            album_title="Unfindable Album",
            status="wanted",
            unfindable_category="artist_absent",
            unfindable_categorised_at=categorised_at,
            last_artist_probe_at=categorised_at,
            last_artist_probe_match_count=0,
        )
        db.seed_request(row)

        result = compose_triage_for_request(42, db)

        assert result is not None
        assert result.unfindable is not None
        self.assertEqual(result.unfindable.category, "artist_absent")
        self.assertEqual(result.unfindable.categorised_at, categorised_at)
        self.assertEqual(result.unfindable.last_artist_probe_match_count, 0)

    def test_unfindable_populated_for_rescued_only(self) -> None:
        """A long-tail rescue clears unfindable_category but still
        populates rescued_at — the unfindable struct must surface so the
        operator sees the audit trail."""
        db = FakePipelineDB()
        rescued_at = _now()
        row = make_request_row(
            id=7,
            status="imported",
            rescued_at=rescued_at,
            prior_unfindable_category="wrong_pressing_available",
        )
        db.seed_request(row)

        result = compose_triage_for_request(7, db)

        assert result is not None
        assert result.unfindable is not None
        self.assertIsNone(result.unfindable.category)
        self.assertEqual(result.unfindable.rescued_at, rescued_at)
        self.assertEqual(
            result.unfindable.prior_unfindable_category,
            "wrong_pressing_available",
        )

    def test_field_quality_carries_resolutions(self) -> None:
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=99))
        db.record_field_resolution(
            request_id=99, field_name="release_group_year",
            status="resolved", reason_code=None,
        )
        db.record_field_resolution(
            request_id=99, field_name="catalog_number",
            status="unresolved_404", reason_code="http_404",
        )

        result = compose_triage_for_request(99, db)

        assert result is not None
        fields = sorted(result.field_quality, key=lambda f: f.field_name)
        self.assertEqual(
            [f.field_name for f in fields],
            ["catalog_number", "release_group_year"],
        )
        self.assertEqual(fields[0].status, "unresolved_404")
        self.assertEqual(fields[0].reason_code, "http_404")
        self.assertEqual(fields[1].status, "resolved")

    def test_search_forensics_summary_and_recent_entries(self) -> None:
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=11))
        # Two found rows + a rejected row with a rejection_reason.
        db.log_search(
            request_id=11, query="artist album", result_count=42,
            outcome="found", candidates=None,
        )
        db.log_search(
            request_id=11, query="artist album wild", result_count=12,
            outcome="rejected", rejection_reason="album_token_missing",
            matcher_score_top1=0.32,
        )
        db.log_search(
            request_id=11, query="artist album",
            result_count=0, outcome="exhausted",
        )

        result = compose_triage_for_request(11, db)

        assert result is not None
        self.assertEqual(result.search_forensics.total_searches, 3)
        self.assertEqual(result.search_forensics.found_count, 1)
        self.assertEqual(result.search_forensics.zero_results_count, 1)
        self.assertEqual(
            result.search_forensics.dominant_rejection_reason,
            "album_token_missing",
        )
        # Newest first.
        self.assertEqual(len(result.search_forensics.recent_entries), 3)
        recent_outcomes = [
            e.outcome for e in result.search_forensics.recent_entries
        ]
        self.assertEqual(
            recent_outcomes, ["exhausted", "rejected", "found"],
        )

    def test_search_forensics_caps_recent_entries_at_ten(self) -> None:
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=12))
        for i in range(15):
            db.log_search(
                request_id=12, query=f"q{i}", result_count=i, outcome="error",
            )

        result = compose_triage_for_request(12, db)

        assert result is not None
        # 15 rows logged; recent_entries is bounded to 10.
        self.assertEqual(result.search_forensics.total_searches, 15)
        self.assertEqual(len(result.search_forensics.recent_entries), 10)


# ---------------------------------------------------------------------------
# list_triage
# ---------------------------------------------------------------------------


class TestListTriage(unittest.TestCase):
    """Cohort listing under filter specs."""

    def _seed_three(self) -> FakePipelineDB:
        db = FakePipelineDB()
        # 1 — healthy.
        db.seed_request(make_request_row(
            id=1, artist_name="Healthy", album_title="Album",
            status="imported", failure_class="resolved",
        ))
        # 2 — unfindable: artist_absent.
        db.seed_request(make_request_row(
            id=2, artist_name="Vanished", album_title="Album",
            status="wanted",
            unfindable_category="artist_absent",
            unfindable_categorised_at=_now(),
        ))
        # 3 — unfindable: wrong_pressing_available, plus a data-quality issue.
        db.seed_request(make_request_row(
            id=3, artist_name="Pressed", album_title="Album",
            status="wanted",
            unfindable_category="wrong_pressing_available",
            unfindable_categorised_at=_now(),
        ))
        db.record_field_resolution(
            request_id=3, field_name="release_group_year",
            status="unresolved_404", reason_code="http_404",
        )
        # 4 — only data-quality issue, no unfindable cohort. Production
        # shape per ``lib/field_resolver_service.py::_classify_lookup_exception``:
        # ``status='unresolved_4xx_client'`` is the sticky-bucket marker
        # operators page on for #374; ``reason_code='http_400'`` is the
        # specific HTTP code the resolver hit.
        db.seed_request(make_request_row(
            id=4, artist_name="DataOnly", album_title="Album", status="wanted",
        ))
        db.record_field_resolution(
            request_id=4, field_name="catalog_number",
            status="unresolved_4xx_client", reason_code="http_400",
        )
        return db

    def test_filter_all(self) -> None:
        db = self._seed_three()
        results = list_triage("all", db, page_size=10)
        ids = sorted(r.request_meta.id for r in results)
        self.assertEqual(ids, [1, 2, 3, 4])

    def test_filter_unfindable(self) -> None:
        db = self._seed_three()
        results = list_triage("unfindable", db, page_size=10)
        ids = sorted(r.request_meta.id for r in results)
        self.assertEqual(ids, [2, 3])

    def test_list_includes_replaced_rows(self) -> None:
        """Pins behavior: replaced rows (frozen audit) ARE included.

        ``status='replaced'`` rows are Replace-action tombstones (see
        ``CLAUDE.md`` invariant #6). Triage cohorts intentionally include
        them so the operator can spot patterns across replacement
        history — e.g. an MBID-shape that keeps tripping HTTP 4xx and
        keeps getting replaced. ``lib/pipeline_db.py::list_triage_page``
        docstring documents this contract; this test pins it.
        """
        db = self._seed_three()
        # Replace request id=2 (the artist_absent unfindable). The old
        # row flips to status='replaced' with unfindable_category still
        # populated (frozen audit shape, mirrors lib/mbid_replace_service).
        replaced_row = db.request(2)
        assert replaced_row is not None
        replaced_row["status"] = "replaced"
        results_all = list_triage("all", db, page_size=10)
        results_unfindable = list_triage("unfindable", db, page_size=10)
        ids_all = sorted(r.request_meta.id for r in results_all)
        ids_unfindable = sorted(
            r.request_meta.id for r in results_unfindable)
        self.assertIn(2, ids_all,
                      "filter='all' must include replaced rows")
        self.assertIn(2, ids_unfindable,
                      "filter='unfindable' must include replaced rows "
                      "whose unfindable_category remains populated")

    def test_filter_unfindable_with_subcategory(self) -> None:
        db = self._seed_three()
        results = list_triage(
            "unfindable:artist_absent", db, page_size=10,
        )
        self.assertEqual([r.request_meta.id for r in results], [2])

    def test_filter_data_quality_bare(self) -> None:
        db = self._seed_three()
        results = list_triage("data_quality", db, page_size=10)
        ids = sorted(r.request_meta.id for r in results)
        # Both 3 and 4 have unresolved field resolutions.
        self.assertEqual(ids, [3, 4])

    def test_filter_data_quality_by_field(self) -> None:
        db = self._seed_three()
        results = list_triage(
            "data_quality:release_group_year", db, page_size=10,
        )
        self.assertEqual([r.request_meta.id for r in results], [3])

    def test_filter_data_quality_by_status_code(self) -> None:
        """#374: the canonical sticky 4xx-client cohort selector.

        ``data_quality:status=unresolved_4xx_client`` matches on the
        ``status`` column — which is what ``lib/field_resolver_service.py``
        actually writes for sticky 4xx errors. Operator workflow:
        ``triage list --filter=data_quality:status=unresolved_4xx_client``.
        """
        db = self._seed_three()
        results = list_triage(
            "data_quality:status=unresolved_4xx_client", db, page_size=10,
        )
        self.assertEqual([r.request_meta.id for r in results], [4])

    def test_filter_data_quality_by_reason_code(self) -> None:
        """The complementary form: filter on the ``reason_code`` column.

        ``reason_code`` carries the specific HTTP code (``http_400`` /
        ``http_410`` / ``http_422`` / etc.). Useful when triaging
        a specific upstream behaviour.
        """
        db = self._seed_three()
        results = list_triage(
            "data_quality:reason=http_400", db, page_size=10,
        )
        self.assertEqual([r.request_meta.id for r in results], [4])

    def test_filter_search_not_converting(self) -> None:
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=5))
        db.seed_request(make_request_row(id=6))
        # Request 5: 3 searches, no founds.
        for _ in range(3):
            db.log_search(
                request_id=5, query="q", result_count=10, outcome="rejected",
            )
        # Request 6: searches with one found row.
        db.log_search(
            request_id=6, query="q", result_count=10, outcome="found",
        )
        results = list_triage(
            "search_not_converting", db, page_size=10,
        )
        self.assertEqual([r.request_meta.id for r in results], [5])

    def test_page_size_and_keyset(self) -> None:
        db = FakePipelineDB()
        for i in range(1, 11):
            db.seed_request(make_request_row(
                id=i, artist_name=f"A{i}", album_title="Album",
            ))
        page1 = list_triage("all", db, page_size=4)
        self.assertEqual([r.request_meta.id for r in page1], [1, 2, 3, 4])
        page2 = list_triage("all", db, page_size=4, after_request_id=4)
        self.assertEqual([r.request_meta.id for r in page2], [5, 6, 7, 8])
        page3 = list_triage("all", db, page_size=4, after_request_id=8)
        self.assertEqual([r.request_meta.id for r in page3], [9, 10])

    def test_invalid_filter_raises(self) -> None:
        db = FakePipelineDB()
        with self.assertRaises(InvalidFilterError):
            list_triage("bogus", db)


# ---------------------------------------------------------------------------
# N+1 guard
# ---------------------------------------------------------------------------


class TestListTriageN1Guard(unittest.TestCase):
    """The cohort path emits 4 queries (+ 1 headroom for future growth)
    regardless of page size.

    Asserted by counting calls on ``FakePipelineDB.query_counts``. The
    list path is bounded to four entries (page + three bulk getters);
    the +1 headroom covers any single optional lookup the service
    might add later.
    """

    def test_list_triage_page_size_50_under_query_cap(self) -> None:
        db = FakePipelineDB()
        for i in range(1, 51):
            db.seed_request(make_request_row(
                id=i, artist_name=f"Artist {i}", album_title="Album",
            ))
            # Field resolutions + search log rows for every request, so
            # the bulk fetchers always have data to load. Without this
            # the test would pass via vacuous "no rows so no queries"
            # paths.
            db.record_field_resolution(
                request_id=i, field_name="release_group_year",
                status="resolved", reason_code=None,
            )
            for _ in range(3):
                db.log_search(
                    request_id=i, query="q", result_count=10,
                    outcome="rejected", rejection_reason="album_token_missing",
                )

        results = list_triage("all", db, page_size=50)
        self.assertEqual(len(results), 50)
        # Four bulk methods + a +1 headroom: page + field_resolutions +
        # search_summaries + recent_search_log.
        total_queries = sum(db.query_counts.values())
        self.assertLessEqual(
            total_queries, 5,
            f"list_triage emitted {total_queries} DB queries "
            f"(breakdown: {db.query_counts}); contract is "
            "4 queries (+ 1 headroom for future growth)",
        )
        # Belt-and-braces — each bulk method must fire exactly once.
        self.assertEqual(db.query_counts.get("list_triage_page"), 1)
        self.assertEqual(
            db.query_counts.get("get_field_resolutions_for_requests"), 1,
        )
        self.assertEqual(
            db.query_counts.get("get_search_summaries_for_requests"), 1,
        )
        self.assertEqual(
            db.query_counts.get("get_recent_search_log_for_requests"), 1,
        )


# ---------------------------------------------------------------------------
# Struct exposed via the wire — keep this tight so refactors fail loudly
# ---------------------------------------------------------------------------


class TestStructShape(unittest.TestCase):
    """Pin the wire-boundary shape of the typed result structs."""

    def test_triage_result_is_frozen_struct(self) -> None:
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1))
        result = compose_triage_for_request(1, db)
        assert result is not None
        self.assertIsInstance(result, TriageResult)
        # Frozen — assignment fails.
        with self.assertRaises((AttributeError, TypeError)):
            result.unfindable = None  # type: ignore[misc]

    def test_search_forensics_summary_has_required_fields(self) -> None:
        # Defensive check: keep the field set explicit so a refactor
        # that drops one of them fails this test before it breaks the
        # operator surface.
        sample = SearchForensicsSummary(
            total_searches=0, with_cands_count=0, found_count=0,
            near_cap_count=0, zero_results_count=0,
            pre_filter_skips_total=0,
            first_strategy_with_cands=None,
            dominant_rejection_reason=None,
            last_search_at=None,
            recent_entries=[],
        )
        self.assertEqual(sample.total_searches, 0)

    def test_search_log_entry_optional_fields(self) -> None:
        entry = SearchLogEntry(
            id=1, created_at=_now(), plan_strategy=None, query=None,
            outcome="error", result_count=None, rejection_reason=None,
            matcher_score_top1=None,
        )
        self.assertEqual(entry.outcome, "error")

    def test_parsed_filter_is_frozen(self) -> None:
        parsed = parse_filter("all")
        self.assertIsInstance(parsed, ParsedTriageFilter)
        with self.assertRaises((AttributeError, TypeError)):
            parsed.kind = "mutated"  # type: ignore[misc]


if __name__ == "__main__":
    unittest.main()
