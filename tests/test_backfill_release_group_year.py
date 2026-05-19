"""Tests for ``scripts/backfill_release_group_year.py``.

U3 / R9 of the search-plan-entropy plan. The backfill is a one-shot
deploy-time script that populates ``album_requests.release_group_year``
from the local MB mirror. Tests use ``FakePipelineDB`` + a stub
``fetch_year`` callable; nothing hits the real mirror.
"""

from __future__ import annotations

import os
import sys
import unittest
from typing import Any

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.backfill_release_group_year import (  # noqa: E402
    BackfillCounters,
    run_backfill,
)
from tests.fakes import FakePipelineDB  # noqa: E402


def _seed(db: FakePipelineDB, **fields: Any) -> int:
    """Seed a request row with sane defaults; return its id."""
    defaults: dict[str, Any] = {
        "artist_name": "Test Artist",
        "album_title": "Test Album",
        "source": "request",
        "mb_release_id": None,
        "mb_release_group_id": None,
        "year": None,
    }
    defaults.update(fields)
    return db.add_request(**defaults)


class TestBackfillHappyPath(unittest.TestCase):
    """The Kid A scenario: row has mb_release_group_id, mirror returns
    a year, the column gets populated."""

    def test_populates_release_group_year_from_mirror(self):
        db = FakePipelineDB()
        rid = _seed(
            db,
            artist_name="Radiohead",
            album_title="Kid A",
            mb_release_group_id="kid-a-rg-mbid",
            year=2008,  # the 2008 reissue
        )

        def fetch(rg_mbid: str) -> int | None:
            self.assertEqual(rg_mbid, "kid-a-rg-mbid")
            return 2000  # the original Kid A first-release year

        counters = run_backfill(
            db=db, fetch_year=fetch, sleep_seconds=0,
        )

        self.assertEqual(counters.populated, 1)
        self.assertEqual(counters.fetched, 1)
        self.assertEqual(counters.no_date, 0)
        self.assertEqual(counters.errors, 0)

        row = db.get_request(rid)
        assert row is not None
        self.assertEqual(row["release_group_year"], 2000)

    def test_skips_rows_without_release_group_id(self):
        """Discogs-only / legacy rows without an MB release-group are
        not visible to the backfill query, so the fetcher is never
        called for them.
        """
        db = FakePipelineDB()
        rid = _seed(
            db, artist_name="A", album_title="B",
            discogs_release_id="12345",  # no mb_release_group_id
        )

        calls: list[str] = []

        def fetch(rg_mbid: str) -> int | None:
            calls.append(rg_mbid)
            return 1999

        counters = run_backfill(
            db=db, fetch_year=fetch, sleep_seconds=0,
        )

        self.assertEqual(counters.fetched, 0)
        self.assertEqual(counters.populated, 0)
        self.assertEqual(calls, [])
        row = db.get_request(rid)
        assert row is not None
        self.assertIsNone(row["release_group_year"])

    def test_dry_run_does_not_write(self):
        db = FakePipelineDB()
        rid = _seed(
            db, artist_name="X", album_title="Y",
            mb_release_group_id="rg-1",
        )

        counters = run_backfill(
            db=db, fetch_year=lambda _: 1995,
            dry_run=True, sleep_seconds=0,
        )

        self.assertEqual(counters.populated, 1)
        row = db.get_request(rid)
        assert row is not None
        self.assertIsNone(row["release_group_year"])


class TestBackfillIdempotency(unittest.TestCase):
    """Re-running the backfill must not re-fetch rows that already have
    a populated ``release_group_year``."""

    def test_second_run_is_a_noop(self):
        db = FakePipelineDB()
        _seed(
            db, artist_name="A", album_title="B",
            mb_release_group_id="rg-idem",
        )

        calls: list[str] = []

        def fetch(rg_mbid: str) -> int | None:
            calls.append(rg_mbid)
            return 1980

        first = run_backfill(db=db, fetch_year=fetch, sleep_seconds=0)
        second = run_backfill(db=db, fetch_year=fetch, sleep_seconds=0)

        self.assertEqual(first.populated, 1)
        self.assertEqual(first.fetched, 1)
        self.assertEqual(second.populated, 0)
        self.assertEqual(second.fetched, 0)
        # The mirror was hit exactly once across both runs.
        self.assertEqual(calls, ["rg-idem"])


class TestBackfillResilience(unittest.TestCase):
    """Per-row failures must not abort the whole batch. The MB mirror
    can return 404 (release-group missing) or no parseable date — both
    must leave the row NULL and the backfill must keep going."""

    def test_mirror_404_leaves_row_null(self):
        db = FakePipelineDB()
        rid_a = _seed(
            db, artist_name="A", album_title="B",
            mb_release_group_id="rg-found",
        )
        rid_b = _seed(
            db, artist_name="C", album_title="D",
            mb_release_group_id="rg-404",
        )
        rid_c = _seed(
            db, artist_name="E", album_title="F",
            mb_release_group_id="rg-also-found",
        )

        def fetch(rg_mbid: str) -> int | None:
            if rg_mbid == "rg-404":
                return None  # simulate 404 / no date
            return 1972 if rg_mbid == "rg-found" else 1985

        counters = run_backfill(
            db=db, fetch_year=fetch, sleep_seconds=0,
        )

        self.assertEqual(counters.fetched, 3)
        self.assertEqual(counters.populated, 2)
        self.assertEqual(counters.no_date, 1)
        self.assertEqual(counters.errors, 0)

        a, b, c = (db.get_request(r) for r in (rid_a, rid_b, rid_c))
        assert a is not None and b is not None and c is not None
        self.assertEqual(a["release_group_year"], 1972)
        self.assertIsNone(b["release_group_year"])
        self.assertEqual(c["release_group_year"], 1985)

    def test_unexpected_exception_per_row_is_recorded_and_continues(self):
        db = FakePipelineDB()
        rid_a = _seed(
            db, artist_name="A", album_title="B",
            mb_release_group_id="rg-good",
        )
        rid_b = _seed(
            db, artist_name="C", album_title="D",
            mb_release_group_id="rg-explode",
        )
        rid_c = _seed(
            db, artist_name="E", album_title="F",
            mb_release_group_id="rg-also-good",
        )

        def fetch(rg_mbid: str) -> int | None:
            if rg_mbid == "rg-explode":
                raise RuntimeError("mirror exploded")
            return 1999

        counters = run_backfill(
            db=db, fetch_year=fetch, sleep_seconds=0,
        )

        self.assertEqual(counters.fetched, 3)
        self.assertEqual(counters.populated, 2)
        self.assertEqual(counters.errors, 1)

        b = db.get_request(rid_b)
        assert b is not None
        self.assertIsNone(b["release_group_year"])
        # The neighbouring rows still got populated.
        a = db.get_request(rid_a)
        c = db.get_request(rid_c)
        assert a is not None and c is not None
        self.assertEqual(a["release_group_year"], 1999)
        self.assertEqual(c["release_group_year"], 1999)


class TestBackfillBatching(unittest.TestCase):
    """The script processes rows in chunks. A chunk's failures must not
    leak into the next chunk — every populated row is committed before
    the next batch starts (FakePipelineDB writes are synchronous, so we
    assert post-batch state after each call)."""

    def test_processes_more_rows_than_one_batch(self):
        db = FakePipelineDB()
        rids = [
            _seed(
                db, artist_name=f"A{i}", album_title=f"Album {i}",
                mb_release_group_id=f"rg-{i}",
            )
            for i in range(7)
        ]

        def fetch(rg_mbid: str) -> int | None:
            # Encode the year as 2000 + the request's index.
            return 2000 + int(rg_mbid.split("-")[1])

        counters = run_backfill(
            db=db, fetch_year=fetch, batch_size=3, sleep_seconds=0,
        )

        self.assertEqual(counters.fetched, 7)
        self.assertEqual(counters.populated, 7)
        for i, rid in enumerate(rids):
            row = db.get_request(rid)
            assert row is not None
            self.assertEqual(row["release_group_year"], 2000 + i)

    def test_limit_caps_total_processed(self):
        db = FakePipelineDB()
        for i in range(10):
            _seed(
                db, artist_name=f"A{i}", album_title=f"X{i}",
                mb_release_group_id=f"rg-{i}",
            )

        counters = run_backfill(
            db=db, fetch_year=lambda _: 1990,
            batch_size=4, limit=5, sleep_seconds=0,
        )

        self.assertEqual(counters.fetched, 5)
        self.assertEqual(counters.populated, 5)


class TestFakePipelineDBExposesField(unittest.TestCase):
    """Downstream consumers (web UI, generator, AlbumRecord builder)
    will read ``release_group_year`` off rows returned by
    ``FakePipelineDB.get_request``. Smoke-test that the field is present
    and writable via ``set_release_group_year`` + ``add_request``."""

    def test_default_value_is_none(self):
        db = FakePipelineDB()
        rid = _seed(db, artist_name="A", album_title="B",
                    mb_release_group_id="rg-default-test")
        row = db.get_request(rid)
        assert row is not None
        self.assertIn("release_group_year", row)
        self.assertIsNone(row["release_group_year"])

    def test_add_request_accepts_explicit_release_group_year(self):
        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="A", album_title="B", source="request",
            mb_release_group_id="rg-1", release_group_year=1973,
        )
        row = db.get_request(rid)
        assert row is not None
        self.assertEqual(row["release_group_year"], 1973)

    def test_set_release_group_year_writes(self):
        db = FakePipelineDB()
        rid = _seed(db, artist_name="A", album_title="B",
                    mb_release_group_id="rg-set-test")
        db.set_release_group_year(rid, 1969)
        row = db.get_request(rid)
        assert row is not None
        self.assertEqual(row["release_group_year"], 1969)


class TestBackfillCountersInit(unittest.TestCase):
    def test_counters_start_at_zero(self):
        c = BackfillCounters()
        self.assertEqual(c.fetched, 0)
        self.assertEqual(c.populated, 0)
        self.assertEqual(c.no_date, 0)
        self.assertEqual(c.errors, 0)


if __name__ == "__main__":
    unittest.main()
