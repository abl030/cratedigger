"""Tests for ``scripts/backfill_field_resolutions.py`` (U3).

The backfill is a deploy-time script that walks every existing request
and resolves the R15 metadata fields via U2's
``lib/field_resolver_service``. Idempotent; re-runnable. Writes every
attempt to ``album_request_field_resolutions`` (migration 030).

Tests use ``FakePipelineDB`` + injected selectors / track-artist writers
so the whole flow runs without touching real PostgreSQL. The resolver
itself is unit-tested in ``tests/test_field_resolver_service.py``; this
file exercises the orchestration logic (row selection, retry-window
gating, per-field dispatch, batching, idempotency).
"""

from __future__ import annotations

import os
import sys
import unittest
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# Bootstrap ephemeral PostgreSQL — sets TEST_DB_DSN env var via side effect
# at import time so the production-cursor regression tests (below) actually
# run instead of being silently skipped (which would fail test_skip_audit).
sys.path.insert(0, os.path.dirname(__file__))
import conftest  # noqa: E402, F401

from lib.field_resolver_service import (  # noqa: E402
    FIELD_CATALOG_NUMBER,
    FIELD_RELEASE_GROUP_ID,
    FIELD_RELEASE_GROUP_YEAR,
    FIELD_TRACK_ARTIST,
    MB_VA_ARTIST_MBID,
    ResolverResult,
)
from scripts.backfill_field_resolutions import (  # noqa: E402
    _is_retry_eligible,
    _RETRY_WINDOWS,
    _select_all_requests_factory,
    _select_requests_needing_resolution_factory,
    BATCH_SIZE,
    FieldBackfillCounters,
    run_backfill,
    run_is_va_compilation,
    run_one_track_structural,
    run_release_group_year,
    run_track_artist,
)
from tests.fakes import FakePipelineDB  # noqa: E402


# --------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------- #


def _seed_request(
    db: FakePipelineDB,
    *,
    artist: str = "Test Artist",
    album: str = "Test Album",
    source: str = "request",
    mb_release_id: str | None = "release-mbid-x",
    mb_release_group_id: str | None = "rg-mbid-x",
    mb_artist_id: str | None = None,
    discogs_release_id: str | None = None,
    release_group_year: int | None = None,
    is_va_compilation: bool = False,
) -> int:
    rid = db.add_request(
        artist_name=artist,
        album_title=album,
        source=source,
        mb_release_id=mb_release_id,
        mb_release_group_id=mb_release_group_id,
        mb_artist_id=mb_artist_id,
        discogs_release_id=discogs_release_id,
        release_group_year=release_group_year,
    )
    # ``add_request`` doesn't accept is_va_compilation; default-False is
    # fine, but tests that exercise the column directly need to set it
    # via the catch-all update.
    db.update_request_fields(rid, is_va_compilation=is_va_compilation)
    # Reset the recorded call list so test assertions only see the
    # writes the backfill makes.
    db.update_request_fields_calls.clear()
    return rid


def _selector_over(rows: list[dict[str, Any]]):
    """Build a Selector that yields the given rows. Used to bypass the
    SQL-backed default selector in tests."""

    def _iter() -> Iterator[dict[str, Any]]:
        for r in rows:
            yield r

    return _iter


def _resolve_to(value: Any) -> Any:
    """A resolver stub that always returns ``ResolverResult(resolved=value)``.
    Records nothing to the side table."""
    def _fn(row: dict[str, Any], db: Any) -> ResolverResult:
        # Mirror the real resolver: record to the side table.
        result = ResolverResult(
            field_name=FIELD_RELEASE_GROUP_YEAR,
            value=value,
            status="resolved",
        )
        db.record_field_resolution(
            request_id=int(row["id"]),
            field_name=FIELD_RELEASE_GROUP_YEAR,
            status=result.status,
            reason_code=result.reason_code,
        )
        return result
    return _fn


# --------------------------------------------------------------------- #
# Retry window
# --------------------------------------------------------------------- #


class TestRetryWindow(unittest.TestCase):
    """Status-window policy is the gate that makes the script idempotent.

    Mirrors the policy pinned in ``lib/field_resolver_service.py`` and
    the U3 section of the plan.
    """

    def test_unresolved_404_blocks_re_attempt_within_30d(self):
        now = datetime(2026, 5, 25, tzinfo=timezone.utc)
        # 25h ago — well within 30d.
        resolved_at = now - timedelta(hours=25)
        self.assertFalse(_is_retry_eligible(
            status="unresolved_404", resolved_at=resolved_at, now=now,
        ))

    def test_unresolved_404_after_31d_is_eligible(self):
        now = datetime(2026, 5, 25, tzinfo=timezone.utc)
        resolved_at = now - timedelta(days=31)
        self.assertTrue(_is_retry_eligible(
            status="unresolved_404", resolved_at=resolved_at, now=now,
        ))

    def test_unresolved_timeout_blocks_then_releases_after_1d(self):
        now = datetime(2026, 5, 25, tzinfo=timezone.utc)
        within = now - timedelta(hours=23)
        beyond = now - timedelta(hours=25)
        self.assertFalse(_is_retry_eligible(
            status="unresolved_timeout", resolved_at=within, now=now,
        ))
        self.assertTrue(_is_retry_eligible(
            status="unresolved_timeout", resolved_at=beyond, now=now,
        ))

    def test_unresolved_malformed_is_permanently_sticky(self):
        now = datetime(2026, 5, 25, tzinfo=timezone.utc)
        # 50 years ago — still NOT eligible.
        resolved_at = now - timedelta(days=365 * 50)
        self.assertFalse(_is_retry_eligible(
            status="unresolved_malformed", resolved_at=resolved_at, now=now,
        ))

    def test_resolved_is_permanently_sticky(self):
        now = datetime(2026, 5, 25, tzinfo=timezone.utc)
        resolved_at = now - timedelta(days=365 * 50)
        self.assertFalse(_is_retry_eligible(
            status="resolved", resolved_at=resolved_at, now=now,
        ))

    def test_unresolved_field_missing_upstream_is_30d(self):
        # The U2 agent's heads-up: MB-side missing-year maps here, not
        # to unresolved_404. The retry window is the longer 30d.
        now = datetime(2026, 5, 25, tzinfo=timezone.utc)
        within = now - timedelta(days=15)
        beyond = now - timedelta(days=31)
        self.assertFalse(_is_retry_eligible(
            status="unresolved_field_missing_upstream",
            resolved_at=within, now=now,
        ))
        self.assertTrue(_is_retry_eligible(
            status="unresolved_field_missing_upstream",
            resolved_at=beyond, now=now,
        ))

    def test_unknown_status_defaults_to_transient_retry(self):
        # A status the table doesn't recognise gets the 1d treatment --
        # safer than "never re-attempt" for an unknown classifier.
        now = datetime(2026, 5, 25, tzinfo=timezone.utc)
        beyond = now - timedelta(hours=25)
        self.assertTrue(_is_retry_eligible(
            status="brand_new_status_2099", resolved_at=beyond, now=now,
        ))

    def test_no_side_table_row_is_always_eligible(self):
        now = datetime(2026, 5, 25, tzinfo=timezone.utc)
        self.assertTrue(_is_retry_eligible(
            status="anything", resolved_at=None, now=now,
        ))

    def test_retry_window_table_covers_all_resolver_statuses(self):
        """Regression guard: any new status in the resolver must come
        with a window entry here. Today's set is fixed."""
        for status in (
            "resolved",
            "unresolved_404",
            "unresolved_field_missing_upstream",
            "unresolved_timeout",
            "unresolved_mirror_unavailable",
            "unresolved_malformed",
        ):
            self.assertIn(status, _RETRY_WINDOWS)


# --------------------------------------------------------------------- #
# release_group_year happy path
# --------------------------------------------------------------------- #


class TestReleaseGroupYearBackfill(unittest.TestCase):
    """The Kid A scenario: row has rg_mbid, resolver returns 2000, the
    column is populated and the side table records a 'resolved' row."""

    def test_happy_path_populates_parent_and_side_table(self):
        db = FakePipelineDB()
        rid = _seed_request(
            db, artist="Radiohead", album="Kid A",
            mb_release_group_id="rg-kid-a",
        )
        row = db.get_request(rid)
        assert row is not None
        selector = _selector_over([row])

        def resolver(req: dict[str, Any], pdb: Any) -> ResolverResult:
            result = ResolverResult(
                field_name=FIELD_RELEASE_GROUP_YEAR,
                value=2000,
                status="resolved",
            )
            pdb.record_field_resolution(
                request_id=int(req["id"]),
                field_name=FIELD_RELEASE_GROUP_YEAR,
                status=result.status,
                reason_code=result.reason_code,
            )
            return result

        counters = run_release_group_year(
            db=db, selector=selector, resolver=resolver,
        )

        self.assertEqual(counters.fetched, 1)
        self.assertEqual(counters.resolved, 1)
        self.assertEqual(counters.unresolved, 0)
        self.assertEqual(counters.errors, 0)
        # Parent column written.
        post = db.get_request(rid)
        assert post is not None
        self.assertEqual(post["release_group_year"], 2000)
        # Side table recorded the attempt.
        side = db.get_field_resolution(rid, FIELD_RELEASE_GROUP_YEAR)
        assert side is not None
        self.assertEqual(side["status"], "resolved")

    def test_unresolved_404_leaves_parent_null(self):
        db = FakePipelineDB()
        rid = _seed_request(
            db, mb_release_group_id="rg-404-test",
        )
        row = db.get_request(rid)
        assert row is not None

        def resolver(req: dict[str, Any], pdb: Any) -> ResolverResult:
            result = ResolverResult(
                field_name=FIELD_RELEASE_GROUP_YEAR,
                status="unresolved_404",
                reason_code="http_404",
            )
            pdb.record_field_resolution(
                request_id=int(req["id"]),
                field_name=FIELD_RELEASE_GROUP_YEAR,
                status=result.status,
                reason_code=result.reason_code,
            )
            return result

        counters = run_release_group_year(
            db=db, selector=_selector_over([row]), resolver=resolver,
        )

        self.assertEqual(counters.resolved, 0)
        self.assertEqual(counters.unresolved, 1)
        post = db.get_request(rid)
        assert post is not None
        self.assertIsNone(post["release_group_year"])
        side = db.get_field_resolution(rid, FIELD_RELEASE_GROUP_YEAR)
        assert side is not None
        self.assertEqual(side["status"], "unresolved_404")

    def test_dry_run_records_side_table_but_skips_parent(self):
        # A dry-run must STILL exercise the resolver (so operators see
        # what would happen), but the parent column must remain NULL.
        # The side table records the attempt because the resolver
        # service writes it -- dry_run only gates the parent update.
        db = FakePipelineDB()
        rid = _seed_request(db, mb_release_group_id="rg-dry")
        row = db.get_request(rid)
        assert row is not None

        def resolver(req: dict[str, Any], pdb: Any) -> ResolverResult:
            result = ResolverResult(
                field_name=FIELD_RELEASE_GROUP_YEAR,
                value=1989, status="resolved",
            )
            pdb.record_field_resolution(
                request_id=int(req["id"]),
                field_name=FIELD_RELEASE_GROUP_YEAR,
                status=result.status, reason_code=None,
            )
            return result

        counters = run_release_group_year(
            db=db, selector=_selector_over([row]),
            resolver=resolver, dry_run=True,
        )

        self.assertEqual(counters.resolved, 1)
        post = db.get_request(rid)
        assert post is not None
        self.assertIsNone(post["release_group_year"])

    def test_resolver_crash_recorded_as_error_but_batch_continues(self):
        db = FakePipelineDB()
        rid_a = _seed_request(db, mb_release_group_id="rg-a")
        rid_b = _seed_request(db, mb_release_group_id="rg-explode")
        rid_c = _seed_request(db, mb_release_group_id="rg-c")
        rows = [db.get_request(r) for r in (rid_a, rid_b, rid_c)]
        rows = [r for r in rows if r is not None]

        def resolver(req: dict[str, Any], pdb: Any) -> ResolverResult:
            rg = req.get("mb_release_group_id")
            if rg == "rg-explode":
                raise RuntimeError("transport blew up")
            result = ResolverResult(
                field_name=FIELD_RELEASE_GROUP_YEAR,
                value=1999, status="resolved",
            )
            pdb.record_field_resolution(
                request_id=int(req["id"]),
                field_name=FIELD_RELEASE_GROUP_YEAR,
                status=result.status, reason_code=None,
            )
            return result

        counters = run_release_group_year(
            db=db, selector=_selector_over(rows), resolver=resolver,
        )

        self.assertEqual(counters.fetched, 3)
        self.assertEqual(counters.resolved, 2)
        self.assertEqual(counters.errors, 1)
        # The neighbour rows still populated.
        post_a = db.get_request(rid_a)
        post_c = db.get_request(rid_c)
        assert post_a is not None and post_c is not None
        self.assertEqual(post_a["release_group_year"], 1999)
        self.assertEqual(post_c["release_group_year"], 1999)


# --------------------------------------------------------------------- #
# Idempotency / retry-window enforcement (the heart of U3)
# --------------------------------------------------------------------- #


class TestSelectorIdempotency(unittest.TestCase):
    """The selector is what makes re-runs cheap. These tests inject a
    custom selector that emulates the real-SQL retry-window join: a row
    with a recent side-table entry is not yielded again."""

    def _make_idempotent_selector(
        self, db: FakePipelineDB, field_name: str,
        now: datetime,
    ):
        """Builds a selector that mirrors the production SQL: yields
        rows whose side-table state says we should retry (or no row
        yet) and whose parent column is still NULL."""

        def _iter() -> Iterator[dict[str, Any]]:
            for rid, row in sorted(db._requests.items()):
                if row.get("release_group_year") is not None:
                    continue
                side = db.get_field_resolution(rid, field_name)
                if side is not None:
                    status = side["status"]
                    resolved_at = side["resolved_at"]
                    if not _is_retry_eligible(
                        status=str(status),
                        resolved_at=resolved_at,
                        now=now,
                    ):
                        continue
                yield dict(row)

        return _iter

    def test_re_running_skips_already_resolved_row(self):
        db = FakePipelineDB()
        rid = _seed_request(db, mb_release_group_id="rg-idem")
        now = datetime(2026, 5, 25, tzinfo=timezone.utc)

        resolver_calls: list[int] = []

        def resolver(req: dict[str, Any], pdb: Any) -> ResolverResult:
            resolver_calls.append(int(req["id"]))
            result = ResolverResult(
                field_name=FIELD_RELEASE_GROUP_YEAR,
                value=1973, status="resolved",
            )
            pdb.record_field_resolution(
                request_id=int(req["id"]),
                field_name=FIELD_RELEASE_GROUP_YEAR,
                status=result.status, reason_code=None,
            )
            return result

        # First run: resolves.
        sel = self._make_idempotent_selector(
            db, FIELD_RELEASE_GROUP_YEAR, now,
        )
        first = run_release_group_year(
            db=db, selector=sel, resolver=resolver,
        )
        self.assertEqual(first.resolved, 1)
        post = db.get_request(rid)
        assert post is not None
        self.assertEqual(post["release_group_year"], 1973)

        # Second run: row is already populated AND side-table says
        # 'resolved' (never re-resolved). The resolver is not called.
        sel2 = self._make_idempotent_selector(
            db, FIELD_RELEASE_GROUP_YEAR, now,
        )
        second = run_release_group_year(
            db=db, selector=sel2, resolver=resolver,
        )
        self.assertEqual(second.fetched, 0)
        self.assertEqual(resolver_calls, [rid])

    def test_unresolved_timeout_retried_after_window(self):
        db = FakePipelineDB()
        rid = _seed_request(db, mb_release_group_id="rg-timeout")
        # Side table recorded 25h ago as a timeout -- should be eligible.
        db.record_field_resolution(
            request_id=rid,
            field_name=FIELD_RELEASE_GROUP_YEAR,
            status="unresolved_timeout",
            reason_code="socket.timeout",
        )
        # Backdate ``resolved_at`` so the retry window has elapsed.
        side = db.field_resolutions[(rid, FIELD_RELEASE_GROUP_YEAR)]
        now = datetime(2026, 5, 25, tzinfo=timezone.utc)
        side.resolved_at = now - timedelta(hours=25)

        def resolver(req: dict[str, Any], pdb: Any) -> ResolverResult:
            result = ResolverResult(
                field_name=FIELD_RELEASE_GROUP_YEAR,
                value=2010, status="resolved",
            )
            pdb.record_field_resolution(
                request_id=int(req["id"]),
                field_name=FIELD_RELEASE_GROUP_YEAR,
                status=result.status, reason_code=None,
            )
            return result

        sel = self._make_idempotent_selector(
            db, FIELD_RELEASE_GROUP_YEAR, now,
        )
        counters = run_release_group_year(
            db=db, selector=sel, resolver=resolver,
        )

        self.assertEqual(counters.fetched, 1)
        self.assertEqual(counters.resolved, 1)
        post = db.get_request(rid)
        assert post is not None
        self.assertEqual(post["release_group_year"], 2010)

    def test_unresolved_404_not_retried_within_30d(self):
        db = FakePipelineDB()
        rid = _seed_request(db, mb_release_group_id="rg-404-recent")
        db.record_field_resolution(
            request_id=rid,
            field_name=FIELD_RELEASE_GROUP_YEAR,
            status="unresolved_404", reason_code="http_404",
        )
        # 25h ago — still inside the 30d sticky window.
        side = db.field_resolutions[(rid, FIELD_RELEASE_GROUP_YEAR)]
        now = datetime(2026, 5, 25, tzinfo=timezone.utc)
        side.resolved_at = now - timedelta(hours=25)

        resolver_calls: list[int] = []

        def resolver(req: dict[str, Any], pdb: Any) -> ResolverResult:
            resolver_calls.append(int(req["id"]))
            raise AssertionError("resolver must NOT be called within the window")

        sel = self._make_idempotent_selector(
            db, FIELD_RELEASE_GROUP_YEAR, now,
        )
        counters = run_release_group_year(
            db=db, selector=sel, resolver=resolver,
        )

        self.assertEqual(counters.fetched, 0)
        self.assertEqual(resolver_calls, [])

    def test_unresolved_malformed_never_retried(self):
        db = FakePipelineDB()
        rid = _seed_request(db, mb_release_group_id="rg-mal")
        db.record_field_resolution(
            request_id=rid,
            field_name=FIELD_RELEASE_GROUP_YEAR,
            status="unresolved_malformed", reason_code="empty_rg_mbid",
        )
        # Even 50 years ago -- malformed is permanent.
        side = db.field_resolutions[(rid, FIELD_RELEASE_GROUP_YEAR)]
        now = datetime(2026, 5, 25, tzinfo=timezone.utc)
        side.resolved_at = now - timedelta(days=365 * 50)

        def resolver(req: dict[str, Any], pdb: Any) -> ResolverResult:
            raise AssertionError("malformed is permanent — resolver must not run")

        sel = self._make_idempotent_selector(
            db, FIELD_RELEASE_GROUP_YEAR, now,
        )
        counters = run_release_group_year(
            db=db, selector=sel, resolver=resolver,
        )
        self.assertEqual(counters.fetched, 0)


# --------------------------------------------------------------------- #
# Batching
# --------------------------------------------------------------------- #


class TestBatching(unittest.TestCase):
    """250 rows through the pipeline; assert every row is touched
    regardless of mid-batch failures."""

    def test_250_rows_all_attempted_even_when_some_fail(self):
        db = FakePipelineDB()
        rids = [
            _seed_request(
                db, artist=f"A{i}", album=f"X{i}",
                mb_release_group_id=f"rg-{i}",
            )
            for i in range(250)
        ]
        rows = [db.get_request(r) for r in rids]
        rows_typed: list[dict[str, Any]] = [r for r in rows if r is not None]

        # Every 50th row "fails" -- the rest resolve.
        def resolver(req: dict[str, Any], pdb: Any) -> ResolverResult:
            if int(req["id"]) % 50 == 0:
                raise RuntimeError("simulated batch failure")
            result = ResolverResult(
                field_name=FIELD_RELEASE_GROUP_YEAR,
                value=1990 + (int(req["id"]) % 30),
                status="resolved",
            )
            pdb.record_field_resolution(
                request_id=int(req["id"]),
                field_name=FIELD_RELEASE_GROUP_YEAR,
                status=result.status, reason_code=None,
            )
            return result

        counters = run_release_group_year(
            db=db, selector=_selector_over(rows_typed), resolver=resolver,
            batch_size=100,
        )

        self.assertEqual(counters.fetched, 250)
        # Every 50th rid (i.e. id values 50, 100, 150, 200, 250) -- but
        # not 0 since rids start at 1. Count those divisible by 50.
        failed = sum(1 for r in rids if r % 50 == 0)
        self.assertEqual(counters.errors, failed)
        self.assertEqual(counters.resolved, 250 - failed)


# --------------------------------------------------------------------- #
# Field-scoped selection
# --------------------------------------------------------------------- #


class TestTrackArtistBackfill(unittest.TestCase):
    """The track_artist backfill walks album_tracks rows. The resolver
    returns one ResolverResult per track; the writer populates each."""

    def test_writer_called_once_per_resolved_track(self):
        db = FakePipelineDB()
        rid = _seed_request(db, mb_release_id="release-tracks")
        db.set_tracks(rid, [
            {"track_number": 1, "title": "Track A"},
            {"track_number": 2, "title": "Track B"},
            {"track_number": 3, "title": "Track C"},
        ])
        row = db.get_request(rid)
        assert row is not None

        def resolver(req: dict[str, Any], pdb: Any) -> list[ResolverResult]:
            return [
                ResolverResult(
                    field_name=FIELD_TRACK_ARTIST,
                    value="Artist A", status="resolved",
                ),
                ResolverResult(
                    field_name=FIELD_TRACK_ARTIST,
                    status="unresolved_field_missing_upstream",
                    reason_code="mb_track_no_artist_credit",
                ),
                ResolverResult(
                    field_name=FIELD_TRACK_ARTIST,
                    value="Artist C", status="resolved",
                ),
            ]

        writes: list[tuple[int, int, int, str]] = []

        def writer(req_id: int, disc: int, tnum: int, artist: str) -> None:
            writes.append((req_id, disc, tnum, artist))

        def get_tracks(req_id: int) -> list[dict[str, Any]]:
            return db.get_tracks(req_id)

        counters = run_track_artist(
            db=db, selector=_selector_over([row]),
            track_artist_writer=writer, get_tracks=get_tracks,
            resolver=resolver,
        )

        self.assertEqual(counters.fetched, 1)
        self.assertEqual(counters.resolved, 1)
        # Two of three tracks got a value; the second one (unresolved)
        # is skipped at the writer.
        self.assertEqual(len(writes), 2)
        self.assertEqual(writes[0], (rid, 1, 1, "Artist A"))
        self.assertEqual(writes[1], (rid, 1, 3, "Artist C"))

    def test_all_tracks_unresolved_counts_as_unresolved(self):
        db = FakePipelineDB()
        rid = _seed_request(db, mb_release_id="release-empty")
        db.set_tracks(rid, [
            {"track_number": 1, "title": "Only Track"},
        ])
        row = db.get_request(rid)
        assert row is not None

        def resolver(req: dict[str, Any], pdb: Any) -> list[ResolverResult]:
            return [ResolverResult(
                field_name=FIELD_TRACK_ARTIST,
                status="unresolved_404", reason_code="http_404",
            )]

        writes: list[Any] = []

        def writer(*args: Any) -> None:
            writes.append(args)

        def get_tracks(req_id: int) -> list[dict[str, Any]]:
            return db.get_tracks(req_id)

        counters = run_track_artist(
            db=db, selector=_selector_over([row]),
            track_artist_writer=writer, get_tracks=get_tracks,
            resolver=resolver,
        )
        self.assertEqual(counters.unresolved, 1)
        self.assertEqual(writes, [])

    def test_dry_run_skips_writes(self):
        db = FakePipelineDB()
        rid = _seed_request(db, mb_release_id="release-dry")
        db.set_tracks(rid, [
            {"track_number": 1, "title": "Track"},
        ])
        row = db.get_request(rid)
        assert row is not None

        def resolver(req: dict[str, Any], pdb: Any) -> list[ResolverResult]:
            return [ResolverResult(
                field_name=FIELD_TRACK_ARTIST,
                value="X", status="resolved",
            )]

        writes: list[Any] = []

        def writer(*args: Any) -> None:
            writes.append(args)

        def get_tracks(req_id: int) -> list[dict[str, Any]]:
            return db.get_tracks(req_id)

        counters = run_track_artist(
            db=db, selector=_selector_over([row]),
            track_artist_writer=writer, get_tracks=get_tracks,
            resolver=resolver, dry_run=True,
        )
        self.assertEqual(counters.resolved, 1)
        self.assertEqual(writes, [])


# --------------------------------------------------------------------- #
# is_va_compilation (the 25-row regression guard)
# --------------------------------------------------------------------- #


class TestIsVaCompilationBackfill(unittest.TestCase):
    """The live DB has 25 wanted rows credited to the canonical VA MBID
    today (column defaults to FALSE). The backfill must flip those 25
    to TRUE without changing any other rows."""

    def test_canonical_mb_va_mbid_flipped_to_true(self):
        db = FakePipelineDB()
        # Seed 25 VA rows + a non-VA control row.
        va_rids = []
        for i in range(25):
            rid = _seed_request(
                db,
                artist=f"Various Artists comp {i}",
                album=f"VA Comp {i}",
                mb_release_id=f"release-va-{i}",
                mb_release_group_id=f"rg-va-{i}",
                mb_artist_id=MB_VA_ARTIST_MBID,
            )
            va_rids.append(rid)
        # Control row: also named "Various Artists" but NOT canonical
        # MBID — must stay FALSE per the regression guard.
        control_rid = _seed_request(
            db,
            artist="Various Artists",
            album="An album",
            mb_artist_id="not-the-canonical-mbid",
        )

        counters, examined, flipped = run_is_va_compilation(
            db=db,
            selector=_selector_over(
                [dict(r) for r in db._requests.values()],
            ),
        )

        self.assertEqual(examined, 26)
        self.assertEqual(flipped, 25)
        self.assertEqual(counters.resolved, 25)

        for rid in va_rids:
            post = db.get_request(rid)
            assert post is not None
            self.assertTrue(post["is_va_compilation"])
        control = db.get_request(control_rid)
        assert control is not None
        self.assertFalse(control["is_va_compilation"])

    def test_re_running_is_idempotent_no_writes_on_correct_rows(self):
        db = FakePipelineDB()
        rid = _seed_request(
            db,
            mb_artist_id=MB_VA_ARTIST_MBID,
            is_va_compilation=True,  # already correct
        )
        # update_request_fields_calls was cleared in _seed_request --
        # any write the backfill makes now is observable.

        counters, examined, flipped = run_is_va_compilation(
            db=db,
            selector=_selector_over([dict(db._requests[rid])]),
        )

        self.assertEqual(examined, 1)
        self.assertEqual(flipped, 0)
        self.assertEqual(counters.resolved, 0)
        # No update was issued because computed == current.
        self.assertEqual(db.update_request_fields_calls, [])

    def test_dry_run_leaves_column_unchanged(self):
        db = FakePipelineDB()
        rid = _seed_request(
            db, mb_artist_id=MB_VA_ARTIST_MBID,
        )
        counters, _, flipped = run_is_va_compilation(
            db=db,
            selector=_selector_over([dict(db._requests[rid])]),
            dry_run=True,
        )
        self.assertEqual(flipped, 1)
        post = db.get_request(rid)
        assert post is not None
        # Still False on disk; the dry-run never wrote.
        self.assertFalse(post["is_va_compilation"])

    def test_release_group_compilation_via_payload_fetcher(self):
        # Rule 2: payload says release-group is a Compilation.
        db = FakePipelineDB()
        rid = _seed_request(
            db, mb_release_group_id="rg-comp",
            mb_artist_id="not-canonical",  # Rule 1 misses
        )

        def rg_payload(rg_mbid: str) -> dict[str, Any]:
            self.assertEqual(rg_mbid, "rg-comp")
            return {"primary-type": "Compilation"}

        counters, _, flipped = run_is_va_compilation(
            db=db,
            selector=_selector_over([dict(db._requests[rid])]),
            mb_release_group_payload_fn=rg_payload,
        )
        self.assertEqual(flipped, 1)
        post = db.get_request(rid)
        assert post is not None
        self.assertTrue(post["is_va_compilation"])

    def test_payload_fetcher_error_falls_back_to_rule_1(self):
        # Rule 1 fires regardless of payload errors.
        db = FakePipelineDB()
        rid = _seed_request(
            db, mb_artist_id=MB_VA_ARTIST_MBID,
            mb_release_group_id="rg-explodes",
        )

        def rg_payload(rg_mbid: str) -> dict[str, Any] | None:
            raise RuntimeError("mirror down")

        counters, _, flipped = run_is_va_compilation(
            db=db,
            selector=_selector_over([dict(db._requests[rid])]),
            mb_release_group_payload_fn=rg_payload,
        )
        # Rule 1 still fires; the payload error is logged but the row
        # is flipped.
        self.assertEqual(flipped, 1)
        self.assertGreaterEqual(counters.errors, 1)
        post = db.get_request(rid)
        assert post is not None
        self.assertTrue(post["is_va_compilation"])


# --------------------------------------------------------------------- #
# one_track_structural
# --------------------------------------------------------------------- #


class TestOneTrackStructuralBackfill(unittest.TestCase):
    """Single-track requests get categorised; multi-track requests
    untouched."""

    def test_single_track_request_categorised(self):
        db = FakePipelineDB()
        single_rid = _seed_request(db)
        # The track count is computed by the selector, but here we
        # build the row dict by hand to inject track_count=1.
        row = dict(db._requests[single_rid])
        row["track_count"] = 1

        counters, examined, cat = run_one_track_structural(
            db=db, selector=_selector_over([row]),
        )

        self.assertEqual(examined, 1)
        self.assertEqual(cat, 1)
        self.assertEqual(counters.resolved, 1)
        post = db.get_request(single_rid)
        assert post is not None
        self.assertEqual(
            post["unfindable_category"], "one_track_structural",
        )
        self.assertIsNotNone(post["unfindable_categorised_at"])

    def test_multi_track_request_untouched(self):
        db = FakePipelineDB()
        rid = _seed_request(db)
        row = dict(db._requests[rid])
        row["track_count"] = 12

        counters, examined, cat = run_one_track_structural(
            db=db, selector=_selector_over([row]),
        )
        self.assertEqual(examined, 1)
        self.assertEqual(cat, 0)
        post = db.get_request(rid)
        assert post is not None
        # add_request doesn't seed the iteration-2 column; assert that
        # the backfill did NOT write it (no update_request_fields call).
        self.assertIsNone(post.get("unfindable_category"))

    def test_already_categorised_row_not_overwritten(self):
        # Operator-set category must not be clobbered by the backfill.
        db = FakePipelineDB()
        rid = _seed_request(db)
        db.update_request_fields(
            rid, unfindable_category="artist_absent",
        )
        row = dict(db._requests[rid])
        row["track_count"] = 1

        counters, _, cat = run_one_track_structural(
            db=db, selector=_selector_over([row]),
        )
        self.assertEqual(cat, 0)
        post = db.get_request(rid)
        assert post is not None
        self.assertEqual(post["unfindable_category"], "artist_absent")

    def test_dry_run_leaves_column_null(self):
        db = FakePipelineDB()
        rid = _seed_request(db)
        row = dict(db._requests[rid])
        row["track_count"] = 1

        counters, _, cat = run_one_track_structural(
            db=db, selector=_selector_over([row]),
            dry_run=True,
        )
        self.assertEqual(cat, 1)
        post = db.get_request(rid)
        assert post is not None
        # add_request doesn't seed unfindable_category; a real backfill
        # would have written it via update_request_fields. Dry-run path
        # must not have called update_request_fields with that key.
        self.assertIsNone(post.get("unfindable_category"))
        written_keys = set()
        for _rid, fields in db.update_request_fields_calls:
            written_keys.update(fields.keys())
        self.assertNotIn("unfindable_category", written_keys)


# --------------------------------------------------------------------- #
# run_backfill orchestration (field-scoped + all)
# --------------------------------------------------------------------- #


class TestRunBackfillOrchestration(unittest.TestCase):
    """The top-level entry-point dispatches per --field. Tests verify the
    selector_factory + writer + payload fetchers chain through to each
    field's runner."""

    def _selector_factory(
        self, db: FakePipelineDB, now: datetime,
    ):
        """Mirror the SQL selector: skip rows past their retry window,
        skip rows with a populated parent column (for resolver-backed
        fields). Walks ``db._requests`` directly so the whole flow runs
        in-memory."""

        def _factory(
            *, field_name: str, parent_column: str | None,
            where_extra: str | None = None,
        ):
            def _iter() -> Iterator[dict[str, Any]]:
                for rid in sorted(db._requests.keys()):
                    row = dict(db._requests[rid])
                    # Track count subquery.
                    row["track_count"] = len(db._tracks.get(rid, []))
                    if parent_column is not None and row.get(parent_column) is not None:
                        continue
                    if field_name == "__all__":
                        if where_extra and "unfindable_category IS NULL" in where_extra:
                            if row.get("unfindable_category") is not None:
                                continue
                        yield row
                        continue
                    # Resolver-backed field: check retry window via side table.
                    side = db.get_field_resolution(rid, field_name)
                    if side is not None and not _is_retry_eligible(
                        status=str(side["status"]),
                        resolved_at=side["resolved_at"], now=now,
                    ):
                        continue
                    yield row
            return _iter

        return _factory

    def test_field_scoped_only_walks_one_field(self):
        db = FakePipelineDB()
        rid_a = _seed_request(
            db, mb_release_id="release-a",
            mb_release_group_id="rg-a",
        )
        db.set_tracks(rid_a, [{"track_number": 1, "title": "T"}])
        now = datetime(2026, 5, 25, tzinfo=timezone.utc)

        writes: list[Any] = []

        def writer(*args: Any) -> None:
            writes.append(args)

        def get_tracks(rid: int) -> list[dict[str, Any]]:
            return db.get_tracks(rid)

        summary = run_backfill(
            db=db,
            field_name=FIELD_TRACK_ARTIST,
            now=now,
            selector_factory=self._selector_factory(db, now),
            track_artist_writer=writer,
            get_tracks_fn=get_tracks,
        )

        # Only the track_artist runner walked.
        self.assertEqual(list(summary.per_field.keys()), [FIELD_TRACK_ARTIST])
        # Other fields' per_field entries weren't populated.
        self.assertNotIn(FIELD_RELEASE_GROUP_YEAR, summary.per_field)

    def test_field_all_runs_every_field(self):
        db = FakePipelineDB()
        _seed_request(
            db,
            mb_release_id="release-x",
            mb_release_group_id="rg-x",
            mb_artist_id=MB_VA_ARTIST_MBID,
        )
        now = datetime(2026, 5, 25, tzinfo=timezone.utc)

        writes: list[Any] = []

        def writer(*args: Any) -> None:
            writes.append(args)

        def get_tracks(rid: int) -> list[dict[str, Any]]:
            return db.get_tracks(rid)

        summary = run_backfill(
            db=db,
            field_name="all",
            now=now,
            selector_factory=self._selector_factory(db, now),
            track_artist_writer=writer,
            get_tracks_fn=get_tracks,
        )

        for expected in (
            FIELD_RELEASE_GROUP_YEAR,
            FIELD_RELEASE_GROUP_ID,
            FIELD_TRACK_ARTIST,
            FIELD_CATALOG_NUMBER,
            "is_va_compilation",
            "one_track_structural",
        ):
            self.assertIn(
                expected, summary.per_field,
                f"--field=all should run {expected!r}",
            )

    def test_unknown_field_raises(self):
        db = FakePipelineDB()
        with self.assertRaises(ValueError):
            run_backfill(
                db=db, field_name="not-a-real-field",
            )


# --------------------------------------------------------------------- #
# FieldBackfillCounters + BackfillSummary
# --------------------------------------------------------------------- #


class TestCounters(unittest.TestCase):
    def test_counters_default_zeros(self):
        c = FieldBackfillCounters(field_name="x")
        self.assertEqual(c.fetched, 0)
        self.assertEqual(c.resolved, 0)
        self.assertEqual(c.unresolved, 0)
        self.assertEqual(c.skipped, 0)
        self.assertEqual(c.errors, 0)
        self.assertEqual(c.field_name, "x")

    def test_summary_string_has_all_counters(self):
        c = FieldBackfillCounters(field_name="track_artist")
        c.fetched = 10
        c.resolved = 7
        c.unresolved = 3
        s = c.summary()
        self.assertIn("track_artist", s)
        self.assertIn("fetched=10", s)
        self.assertIn("resolved=7", s)
        self.assertIn("unresolved=3", s)

    def test_batch_size_constant_is_100(self):
        # The plan pins "Progress: prints ... every 100 rows".
        self.assertEqual(BATCH_SIZE, 100)


class TestAdvisoryLock(unittest.TestCase):
    """The backfill script must take the BFIL advisory lock for its run.

    The deploy runbook stops cratedigger/cratedigger-importer/cratedigger-web
    before running the script — this lock is belt-and-braces against any
    stray writer outside that procedure.
    """

    def test_advisory_lock_namespace_constant_pinned(self):
        # Pin both the value and the fact that lib.pipeline_db is the
        # canonical declaration site (per the existing namespace
        # convention).
        from lib.pipeline_db import ADVISORY_LOCK_NAMESPACE_BACKFILL
        self.assertEqual(ADVISORY_LOCK_NAMESPACE_BACKFILL, 0x4246494C)

    def test_main_acquires_backfill_lock_around_run_backfill(self):
        """main() takes ADVISORY_LOCK_NAMESPACE_BACKFILL before invoking
        run_backfill and releases on exit."""
        import importlib
        import scripts.backfill_field_resolutions as script_mod
        importlib.reload(script_mod)

        from tests.fakes import FakePipelineDB
        from lib.pipeline_db import ADVISORY_LOCK_NAMESPACE_BACKFILL

        # Spy: record advisory_lock acquisition + release ordering
        # against the underlying mechanism.
        fake_db = FakePipelineDB()
        events = []
        original_lock = fake_db.advisory_lock

        from contextlib import contextmanager

        @contextmanager
        def spy_lock(namespace, key):
            events.append(("acquire", namespace, key))
            with original_lock(namespace, key) as acquired:
                yield acquired
            events.append(("release", namespace, key))

        fake_db.advisory_lock = spy_lock  # type: ignore[method-assign]

        # PipelineDB is imported inside main(); patch at the source so
        # the lazy import returns the fake.
        with mock.patch(
            "lib.pipeline_db.PipelineDB", return_value=fake_db,
        ):
            with mock.patch.object(
                script_mod, "run_backfill",
            ) as mock_run:
                mock_run.return_value = script_mod.BackfillSummary()
                rc = script_mod.main(
                    ["--field=release_group_year", "--dry-run"],
                )

        self.assertEqual(rc, 0)
        # Lock acquired BEFORE run_backfill, released AFTER.
        self.assertGreaterEqual(len(events), 2)
        self.assertEqual(
            events[0], ("acquire", ADVISORY_LOCK_NAMESPACE_BACKFILL, 0),
        )
        self.assertEqual(
            events[-1], ("release", ADVISORY_LOCK_NAMESPACE_BACKFILL, 0),
        )
        # Acquisition happened before run_backfill was called.
        mock_run.assert_called_once()

    def test_main_returns_5_when_lock_already_held(self):
        """Concurrent backfill protection: if the lock can't be acquired,
        main() exits 5 (the transient/retry convention) and does NOT
        invoke run_backfill."""
        import importlib
        import scripts.backfill_field_resolutions as script_mod
        importlib.reload(script_mod)

        from tests.fakes import FakePipelineDB
        from contextlib import contextmanager

        fake_db = FakePipelineDB()

        @contextmanager
        def lock_busy(namespace, key):
            yield False  # advisory_lock returns False when already held

        fake_db.advisory_lock = lock_busy  # type: ignore[method-assign]

        with mock.patch(
            "lib.pipeline_db.PipelineDB", return_value=fake_db,
        ):
            with mock.patch.object(
                script_mod, "run_backfill",
            ) as mock_run:
                rc = script_mod.main(
                    ["--field=release_group_year", "--dry-run"],
                )

        self.assertEqual(rc, 5)
        mock_run.assert_not_called()


TEST_DSN = os.environ.get("TEST_DB_DSN")


def _requires_postgres(cls):
    if not TEST_DSN:
        return unittest.skip(
            "TEST_DB_DSN not set — skipping production-cursor regression tests"
        )(cls)
    return cls


@_requires_postgres
class TestProductionCursorPathYieldsDictRows(unittest.TestCase):
    """Regression guard for the dict(row) crash on a default tuple cursor.

    The unit tests above all inject a Selector that walks
    FakePipelineDB._requests (already dict-shaped). The production paths
    in _select_requests_needing_resolution_factory and
    _select_all_requests_factory open raw psycopg2 cursors and call
    dict(row) on each result. With a default tuple cursor, dict(tuple)
    crashes with TypeError on the first row. RealDictCursor is required.
    These tests exercise the real cursor path against an actual
    PostgreSQL test DB so the next regression surfaces immediately.
    """

    def setUp(self):
        from lib import pipeline_db
        self.db = pipeline_db.PipelineDB(TEST_DSN)
        # Clean slate for both tables this regression touches.
        for table in (
            "album_request_field_resolutions",
            "album_tracks",
            "album_requests",
        ):
            self.db._execute(f"TRUNCATE {table} CASCADE")
        self.db.conn.commit()
        # Seed one request to make the cursor path actually yield a row.
        self.req_id = self.db.add_request(
            mb_release_id="cursor-regression-mbid",
            artist_name="Cursor",
            album_title="Regression",
            source="request",
        )

    def tearDown(self):
        self.db.close()

    def test_select_all_requests_factory_yields_dict_rows(self):
        """Without RealDictCursor, dict(row) crashes on the first
        row. This test exercises the production code path end-to-end —
        no Selector injection, no FakePipelineDB."""
        sel = _select_all_requests_factory(
            db=self.db, limit=10,
        )
        rows = list(sel())
        self.assertEqual(len(rows), 1)
        # The row MUST be a dict-shaped object the consumer code can
        # access by column name. A raw tuple here would silently work
        # with .get() returning None for every key (the failure mode
        # the unit tests miss).
        self.assertIsInstance(rows[0], dict)
        self.assertEqual(rows[0]["id"], self.req_id)
        self.assertEqual(rows[0]["mb_release_id"], "cursor-regression-mbid")
        self.assertEqual(rows[0]["artist_name"], "Cursor")

    def test_select_requests_needing_resolution_factory_yields_dict_rows(self):
        """Same regression for the retry-eligibility selector — the
        other raw-cursor path in the backfill script."""
        # Construct the simplest path: parent column NULL, no side-table
        # row → row is eligible.
        sel = _select_requests_needing_resolution_factory(
            db=self.db,
            parent_column="release_group_year",
            field_name="release_group_year",
            limit=10,
            now=datetime.now(timezone.utc),
        )
        rows = list(sel())
        # The seeded request has release_group_year NULL → should yield.
        self.assertEqual(len(rows), 1)
        self.assertIsInstance(rows[0], dict)
        self.assertEqual(rows[0]["id"], self.req_id)
        self.assertEqual(rows[0]["mb_release_id"], "cursor-regression-mbid")


if __name__ == "__main__":
    unittest.main()
