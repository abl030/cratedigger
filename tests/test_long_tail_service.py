"""Tests for ``lib.long_tail_service`` — the Long-Tail Triage Console
worklist read backend (U1).

Two layers:

* Service-level banding logic against ``FakePipelineDB`` + an injected
  counting ``band_fn`` (the N+1 guard counts both the cohort query AND
  the beets membership / detail queries the real ``band_fn`` issues).
* A real-PG round-trip (``TestLongTailCohortRoundTrip``) asserting that
  ``in_flight_rescue`` and the projected columns survive the production
  ``get_long_tail_cohort`` / ``get_long_tail_request`` queries — per
  test-fidelity Rule A. Written FIRST (RED) before the DB method existed.
"""

from __future__ import annotations

import os
import sys
import unittest
import uuid
from datetime import datetime, timezone

import msgspec

# Bootstrap ephemeral PostgreSQL if available (sets TEST_DB_DSN).
sys.path.append(os.path.dirname(__file__))
import conftest  # noqa: F401,E402

from lib.long_tail_service import (  # noqa: E402
    BAND_MISSING,
    LongTailResult,
    LongTailRow,
    band_one_long_tail,
    list_long_tail,
)

# In-library-but-unrankable band, produced by ``compute_library_rank`` /
# the injected band_fn (not a service constant).
BAND_UNKNOWN = "unknown"
from tests.fakes import FakePipelineDB  # noqa: E402
from tests.helpers import make_request_row  # noqa: E402

TEST_DSN = os.environ.get("TEST_DB_DSN")


def requires_postgres(cls):
    """Gate a PG round-trip class on TEST_DB_DSN.

    The nix-shell dev shell always provides ephemeral PostgreSQL (initdb
    + pg_ctl), so this never actually skips in CI / local runs — it is a
    last-resort guard for an environment with the tools genuinely absent.
    Mirrors ``tests/test_pipeline_db.py::requires_postgres`` — the
    non-decorator helper form the skip-audit allows.
    """
    if not TEST_DSN:
        return unittest.skip("TEST_DB_DSN not set")(cls)
    return cls


def _fixed_band_fn(mapping: dict[str, str]):
    """Return a band_fn that maps each release id to its band per
    ``mapping``; release ids absent from ``mapping`` are simply omitted
    from the returned dict (the service bands them ``Missing``)."""

    def _fn(release_ids: list[str]) -> dict[str, str]:
        return {rid: mapping[rid] for rid in release_ids if rid in mapping}

    return _fn


# ---------------------------------------------------------------------------
# Service-level banding
# ---------------------------------------------------------------------------


class TestListLongTailBanding(unittest.TestCase):
    def test_missing_when_no_beets_album(self) -> None:
        """AE1: a wanted request whose release isn't in the library bands
        Missing; an imported request is absent from the result."""
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=1, status="wanted", mb_release_id="rel-1",
            artist_name="A", album_title="Album"))
        db.seed_request(make_request_row(
            id=2, status="imported", mb_release_id="rel-2"))
        # Band fn returns nothing → not in library → Missing.
        result = list_long_tail(db, _fixed_band_fn({}))
        self.assertIsInstance(result, LongTailResult)
        self.assertEqual(result.outcome, "ok")
        self.assertEqual([r.id for r in result.rows], [1])
        self.assertEqual(result.rows[0].band, BAND_MISSING)

    def test_transparent_on_disk(self) -> None:
        """AE2: a wanted request whose beets copy classifies Transparent
        bands Transparent."""
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=1, status="wanted", mb_release_id="rel-1"))
        result = list_long_tail(
            db, _fixed_band_fn({"rel-1": "transparent"}))
        self.assertEqual(result.rows[0].band, "transparent")

    def test_present_but_rank_unknown_bands_unknown(self) -> None:
        """In-library-but-unclassifiable bands Unknown, not Missing.

        The band_fn returns ``"unknown"`` for a release that IS in the
        membership set but whose detail row can't be ranked — distinct
        from absent-from-membership (which bands Missing)."""
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=1, status="wanted", mb_release_id="rel-present"))
        db.seed_request(make_request_row(
            id=2, status="wanted", mb_release_id="rel-absent"))
        result = list_long_tail(
            db, _fixed_band_fn({"rel-present": BAND_UNKNOWN}))
        by_id = {r.id: r for r in result.rows}
        # Present-but-rank-unknown → Unknown.
        self.assertEqual(by_id[1].band, BAND_UNKNOWN)
        # Absent-from-membership → Missing (the other mechanism).
        self.assertEqual(by_id[2].band, BAND_MISSING)

    def test_track_count_and_spectral_project_onto_row(self) -> None:
        """The card meta (N tracks) + on-disk spectral strip read straight
        off the cohort projection — ``track_count`` counts ``album_tracks``;
        ``current_spectral_grade`` / ``current_spectral_bitrate`` mirror the
        denormalised request columns. NULL spectral stays NULL ("if known").
        """
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=1, status="wanted", mb_release_id="rel-1",
            current_spectral_grade="genuine", current_spectral_bitrate=952))
        db.set_tracks(1, [
            {"track_number": 1, "title": "A"},
            {"track_number": 2, "title": "B"},
            {"track_number": 3, "title": "C"},
        ])
        # A second wanted row with no tracks + unknown spectral.
        db.seed_request(make_request_row(
            id=2, status="wanted", mb_release_id="rel-2"))
        rows = {r.id: r for r in list_long_tail(db, _fixed_band_fn({})).rows}
        self.assertEqual(rows[1].track_count, 3)
        self.assertEqual(rows[1].current_spectral_grade, "genuine")
        self.assertEqual(rows[1].current_spectral_bitrate, 952)
        # No tracks → 0; unknown spectral → None.
        self.assertEqual(rows[2].track_count, 0)
        self.assertIsNone(rows[2].current_spectral_grade)
        self.assertIsNone(rows[2].current_spectral_bitrate)

    def test_mb_release_group_id_projects_onto_row(self) -> None:
        """The cohort row carries ``mb_release_group_id`` so the console's
        accept-sibling control + siblings panel read it straight off the
        worklist row — no client-side stamp from the pipeline-detail fetch,
        and the single-row refetch-and-patch (KTD8) can't drop it (#398)."""
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=1, status="wanted", mb_release_id="rel-1",
            mb_release_group_id="rg-1"))
        db.seed_request(make_request_row(
            id=2, status="wanted", mb_release_id="rel-2"))
        rows = {r.id: r for r in list_long_tail(db, _fixed_band_fn({})).rows}
        self.assertEqual(rows[1].mb_release_group_id, "rg-1")
        self.assertIsNone(rows[2].mb_release_group_id)

    def test_discogs_sourced_row_bands_via_dual_key_lookup(self) -> None:
        """A Discogs-sourced wanted request bands correctly — the
        mb_release_id carries the Discogs numeric, banded the same way
        (no new lookup path; KTD7 only restricts accept-sibling, not
        banding)."""
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=1, status="wanted", source="request",
            mb_release_id="12856590", discogs_release_id="12856590"))
        result = list_long_tail(
            db, _fixed_band_fn({"12856590": "excellent"}))
        self.assertEqual(result.rows[0].band, "excellent")
        self.assertEqual(result.rows[0].discogs_release_id, "12856590")

    def test_in_flight_rescue_stamp(self) -> None:
        """An active youtube_running download_log row → in_flight_rescue
        True; a row without → False."""
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=1, status="wanted", mb_release_id="rel-1"))
        db.seed_request(make_request_row(
            id=2, status="wanted", mb_release_id="rel-2"))
        db.insert_youtube_running(
            request_id=2, browse_id="MPREb_a", audio_playlist_id=None,
            yt_url="https://music.youtube.com/playlist?list=a",
            expected_track_count=12,
        )
        result = list_long_tail(db, _fixed_band_fn({}))
        by_id = {r.id: r for r in result.rows}
        self.assertFalse(by_id[1].in_flight_rescue)
        self.assertTrue(by_id[2].in_flight_rescue)

    def test_band_filter_narrows_to_single_band(self) -> None:
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=1, status="wanted", mb_release_id="rel-1"))
        db.seed_request(make_request_row(
            id=2, status="wanted", mb_release_id="rel-2"))
        db.seed_request(make_request_row(
            id=3, status="wanted", mb_release_id="rel-3"))
        band_fn = _fixed_band_fn({
            "rel-2": "transparent", "rel-3": "transparent",
        })
        # rel-1 absent → missing; rel-2/3 transparent.
        result = list_long_tail(db, band_fn, band="transparent")
        self.assertEqual([r.id for r in result.rows], [2, 3])
        self.assertEqual(result.band_filter, "transparent")

        missing = list_long_tail(db, band_fn, band=BAND_MISSING)
        self.assertEqual([r.id for r in missing.rows], [1])


class TestBandOneLongTail(unittest.TestCase):
    def test_single_id_bands_one_request(self) -> None:
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=7, status="wanted", mb_release_id="rel-7"))
        row = band_one_long_tail(
            db, _fixed_band_fn({"rel-7": "good"}), 7)
        assert row is not None
        self.assertIsInstance(row, LongTailRow)
        self.assertEqual(row.id, 7)
        self.assertEqual(row.band, "good")

    def test_single_id_missing_or_not_wanted_returns_none(self) -> None:
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=8, status="imported", mb_release_id="rel-8"))
        self.assertIsNone(band_one_long_tail(db, _fixed_band_fn({}), 8))
        self.assertIsNone(band_one_long_tail(db, _fixed_band_fn({}), 999))


class TestListLongTailN1Guard(unittest.TestCase):
    """The cohort path's total query count is constant regardless of
    cohort size — counting BOTH the Postgres cohort query AND the beets
    membership + check_mbids_detail batch (modelled on
    ``TestListTriageN1Guard``)."""

    def test_query_count_constant_across_cohort_size(self) -> None:
        db = FakePipelineDB()
        band_calls: list[list[str]] = []

        def counting_band_fn(release_ids: list[str]) -> dict[str, str]:
            # The real band_fn issues exactly two beets queries
            # (membership + check_mbids_detail). We record each batch
            # call here and assert it fires once for the whole cohort.
            band_calls.append(list(release_ids))
            return {rid: "transparent" for rid in release_ids}

        for i in range(1, 51):
            db.seed_request(make_request_row(
                id=i, status="wanted", mb_release_id=f"rel-{i}",
                artist_name=f"Artist {i}", album_title="Album"))

        result = list_long_tail(db, counting_band_fn)
        self.assertEqual(len(result.rows), 50)
        # Exactly one Postgres cohort query.
        self.assertEqual(db.query_counts.get("get_long_tail_cohort"), 1)
        self.assertEqual(sum(db.query_counts.values()), 1)
        # Exactly one batched band call for the whole cohort — never per
        # row. This stands in for the two beets queries the real band_fn
        # issues against the whole mb_release_id list.
        self.assertEqual(len(band_calls), 1)
        self.assertEqual(len(band_calls[0]), 50)


# ---------------------------------------------------------------------------
# Real-PG round-trip (test-fidelity Rule A) — written RED first
# ---------------------------------------------------------------------------


@requires_postgres
class TestLongTailCohortRoundTrip(unittest.TestCase):
    """Production-query round-trip: the projected columns + in_flight_rescue
    survive ``get_long_tail_cohort`` / ``get_long_tail_request``, and a
    row populated with real datetime / uuid serializes through the
    service → ``msgspec.to_builtins`` without error (datetime-500 guard).
    """

    def setUp(self) -> None:
        from lib import pipeline_db
        self.db = pipeline_db.PipelineDB(TEST_DSN)
        for table in ("download_log", "album_requests"):
            self.db._execute(f"TRUNCATE {table} CASCADE")
        self.db.conn.commit()

    def tearDown(self) -> None:
        self.db.close()

    def test_cohort_query_stamps_in_flight_rescue_and_projects_columns(self):
        rel_uuid = str(uuid.uuid4())
        rg_uuid = str(uuid.uuid4())
        rid_plain = self.db.add_request(
            artist_name="Vanishing Artist", album_title="Lost Pressing",
            source="request", mb_release_id=rel_uuid,
            mb_release_group_id=rg_uuid, year=1972,
            status="wanted")
        # Tracks (counted into track_count) + denormalised on-disk spectral.
        self.db.set_tracks(rid_plain, [
            {"track_number": 1, "title": "I"},
            {"track_number": 2, "title": "II"},
        ])
        self.db.update_request_fields(
            rid_plain, current_spectral_grade="genuine",
            current_spectral_bitrate=941)
        rid_rescue = self.db.add_request(
            artist_name="Rescue Artist", album_title="Found On YouTube",
            source="request", mb_release_id=str(uuid.uuid4()),
            status="wanted")
        # An imported request must NOT appear in the cohort.
        self.db.add_request(
            artist_name="Done", album_title="Imported",
            source="request", mb_release_id=str(uuid.uuid4()),
            status="imported")
        # In-flight youtube rescue on rid_rescue.
        self.db.insert_youtube_running(
            request_id=rid_rescue, browse_id="MPREb_rt",
            audio_playlist_id=None,
            yt_url="https://music.youtube.com/playlist?list=rt",
            expected_track_count=10,
        )

        rows = self.db.get_long_tail_cohort()
        by_id = {r["id"]: r for r in rows}
        self.assertEqual(set(by_id), {rid_plain, rid_rescue})

        plain = by_id[rid_plain]
        # Every projected column round-trips.
        self.assertEqual(plain["artist_name"], "Vanishing Artist")
        self.assertEqual(plain["album_title"], "Lost Pressing")
        self.assertEqual(plain["year"], 1972)
        self.assertEqual(plain["status"], "wanted")
        self.assertEqual(plain["mb_release_id"], rel_uuid)
        # The rg id backs the accept-sibling control + siblings panel (#398).
        self.assertEqual(plain["mb_release_group_id"], rg_uuid)
        self.assertIsNone(by_id[rid_rescue]["mb_release_group_id"])
        self.assertIn("target_format", plain)
        self.assertIn("min_bitrate", plain)
        self.assertIn("search_filetype_override", plain)
        self.assertIn("unfindable_category", plain)
        # track_count counts album_tracks; spectral mirrors the request cols.
        self.assertEqual(plain["track_count"], 2)
        self.assertEqual(plain["current_spectral_grade"], "genuine")
        self.assertEqual(plain["current_spectral_bitrate"], 941)
        # The no-tracks rescue row counts 0, not NULL.
        self.assertEqual(by_id[rid_rescue]["track_count"], 0)
        self.assertIsNone(by_id[rid_rescue]["current_spectral_grade"])
        # in_flight_rescue stamped correctly by the EXISTS predicate.
        self.assertFalse(plain["in_flight_rescue"])
        self.assertTrue(by_id[rid_rescue]["in_flight_rescue"])

    def test_single_id_query_round_trips(self) -> None:
        rid = self.db.add_request(
            artist_name="Solo", album_title="One",
            source="request", mb_release_id=str(uuid.uuid4()),
            status="wanted")
        row = self.db.get_long_tail_request(rid)
        assert row is not None
        self.assertEqual(row["id"], rid)
        self.assertFalse(row["in_flight_rescue"])
        # Non-wanted / missing → None.
        self.db.add_request(
            artist_name="X", album_title="Y", source="request",
            mb_release_id=str(uuid.uuid4()), status="imported")
        self.assertIsNone(self.db.get_long_tail_request(999999))

    def test_service_serializes_real_row_without_error(self) -> None:
        """The datetime-500 guard: a real production row routed through
        the service serializes via ``msgspec.to_builtins`` cleanly."""
        rid = self.db.add_request(
            artist_name="Ser", album_title="Ialize",
            source="request", mb_release_id=str(uuid.uuid4()),
            status="wanted", year=2001)

        def band_fn(release_ids: list[str]) -> dict[str, str]:
            return {rid_: "poor" for rid_ in release_ids}

        result = list_long_tail(self.db, band_fn)
        self.assertEqual([r.id for r in result.rows], [rid])
        # to_builtins must not raise on the real row's value types.
        builtins = msgspec.to_builtins(result.rows)
        self.assertEqual(builtins[0]["band"], "poor")
        # And it round-trips back into the Struct.
        back = msgspec.convert(builtins[0], type=LongTailRow)
        self.assertEqual(back.id, rid)
        self.assertEqual(back.year, 2001)


if __name__ == "__main__":
    unittest.main()
