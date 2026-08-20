"""Tests for ``lib.long_tail_service`` — the Long-Tail Triage Console
worklist read backend (U1).

Two layers:

* Service-level banding logic against ``FakePipelineDB`` + an injected
  counting ``band_fn`` (the N+1 guard counts the cohort query plus the
  single coherent Beets resolver batch the real ``band_fn`` issues).
* A real-PG round-trip (``TestLongTailCohortRoundTrip``) asserting that
  ``in_flight_rescue`` and the projected columns survive the production
  ``get_long_tail_cohort`` / ``get_long_tail_request`` queries — per
  test-fidelity Rule A. Written FIRST (RED) before the DB method existed.
"""

from __future__ import annotations

import os
import sqlite3
import sys
import unittest
import uuid

import msgspec

# Bootstrap ephemeral PostgreSQL if available (sets TEST_DB_DSN).
sys.path.append(os.path.dirname(__file__))
import conftest  # noqa: F401

from lib.banding import (
    CurrentBeetsBandingAmbiguityError,
    CurrentBeetsBandingIdentityError,
    CurrentBeetsBandingUnavailableError,
)
from lib.beets_db import CurrentBeetsAmbiguous
from lib.long_tail_service import (
    BAND_MISSING,
    LongTailBandingUnavailableError,
    LongTailIdentityError,
    LongTailResult,
    LongTailRow,
    band_one_long_tail,
    classify_long_tail_failure,
    list_long_tail,
)
from lib.release_identity import (
    ConflictingReleaseIdentityError,
    ReleaseIdentity,
)

# In-library-but-unrankable band, produced by ``compute_library_rank`` /
# the injected band_fn (not a service constant).
BAND_UNKNOWN = "unknown"
from tests.fakes import FakePipelineDB
from tests.helpers import (
    REQUEST_CASCADE_RESET_TABLES,
    delete_all_rows,
    make_request_row,
)

TEST_DSN = os.environ.get("TEST_DB_DSN")

MB_RELEASE_1 = "00000000-0000-0000-0000-000000000001"
MB_RELEASE_2 = "00000000-0000-0000-0000-000000000002"
DISCOGS_RELEASE = "12856590"


def _mb_release(sequence: int) -> str:
    return f"00000000-0000-0000-0000-{sequence:012d}"


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
    ``mapping``; ids absent from ``mapping`` receive an explicit Beets
    ``Missing`` answer."""

    def _fn(release_ids: list[str]) -> dict[str, str]:
        return {rid: mapping.get(rid, BAND_MISSING) for rid in release_ids}

    return _fn


def _recording_empty_band_fn(
    batches: list[list[str]],
):
    def _fn(release_ids: list[str]) -> dict[str, str]:
        batches.append(list(release_ids))
        return {}

    return _fn


# ---------------------------------------------------------------------------
# Service-level banding
# ---------------------------------------------------------------------------


class TestLongTailFailureClassification(unittest.TestCase):
    def test_expected_failures_have_one_stable_public_mapping(self) -> None:
        identity = ReleaseIdentity(
            source="musicbrainz",
            release_id=MB_RELEASE_1,
        )
        busy = sqlite3.OperationalError("database is busy")
        busy.sqlite_errorcode = sqlite3.SQLITE_BUSY
        cases = (
            (
                "missing authority",
                FileNotFoundError("Beets DB not found"),
                "unavailable",
                "long_tail_authority_unavailable",
                503,
                5,
            ),
            (
                "locked authority",
                busy,
                "unavailable",
                "long_tail_authority_unavailable",
                503,
                5,
            ),
            (
                "resolver omitted identity",
                CurrentBeetsBandingUnavailableError("omitted"),
                "unavailable",
                "long_tail_authority_unavailable",
                503,
                5,
            ),
            (
                "band map omitted identity",
                LongTailBandingUnavailableError("omitted"),
                "unavailable",
                "long_tail_authority_unavailable",
                503,
                5,
            ),
            (
                "ambiguous current topology",
                CurrentBeetsBandingAmbiguityError((CurrentBeetsAmbiguous(
                    identity=identity,
                    album_ids=(1, 2),
                    reason="multiple_matches",
                ),)),
                "conflict",
                "long_tail_authority_conflict",
                409,
                4,
            ),
            (
                "invalid banding identity",
                CurrentBeetsBandingIdentityError("invalid"),
                "conflict",
                "long_tail_authority_conflict",
                409,
                4,
            ),
            (
                "conflicting exact authority",
                ConflictingReleaseIdentityError("conflict"),
                "conflict",
                "long_tail_authority_conflict",
                409,
                4,
            ),
            (
                "invalid request identity",
                LongTailIdentityError("invalid"),
                "conflict",
                "long_tail_authority_conflict",
                409,
                4,
            ),
        )
        for (
            name,
            failure,
            category,
            error,
            http_status,
            cli_exit_code,
        ) in cases:
            with self.subTest(name=name):
                classified = classify_long_tail_failure(failure)
                self.assertIsNotNone(classified)
                assert classified is not None
                self.assertEqual(classified.category, category)
                self.assertEqual(classified.error, error)
                self.assertEqual(classified.http_status, http_status)
                self.assertEqual(classified.cli_exit_code, cli_exit_code)

    def test_unexpected_schema_and_programmer_failures_remain_unclassified(
        self,
    ) -> None:
        schema = sqlite3.OperationalError("no such table: albums")
        schema.sqlite_errorcode = sqlite3.SQLITE_ERROR

        self.assertIsNone(classify_long_tail_failure(schema))
        self.assertIsNone(classify_long_tail_failure(
            RuntimeError("programmer defect")
        ))


class TestListLongTailBanding(unittest.TestCase):
    def test_modern_discogs_only_row_uses_its_exact_release_identity(self) -> None:
        """A modern Discogs request has no compatibility MB field."""
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=1,
            status="wanted",
            mb_release_id=None,
            discogs_release_id="0012856590",
        ))
        batches: list[list[str]] = []

        def band_fn(release_ids: list[str]) -> dict[str, str]:
            batches.append(list(release_ids))
            return {DISCOGS_RELEASE: "excellent"}

        result = list_long_tail(db, band_fn)

        self.assertEqual(batches, [[DISCOGS_RELEASE]])
        self.assertEqual(result.rows[0].band, "excellent")

    def test_valid_identity_without_authoritative_band_fails_closed(self) -> None:
        """An omitted authority result is not evidence of Beets absence."""
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=1, status="wanted", mb_release_id=MB_RELEASE_1,
        ))

        with self.assertRaisesRegex(RuntimeError, MB_RELEASE_1):
            list_long_tail(db, lambda _release_ids: {})

    def test_invalid_request_identity_never_reaches_beets_authority(self) -> None:
        """Missing, malformed, and conflicting identities fail closed."""
        cases = (
            (None, None),
            ("not-a-release-id", None),
            (MB_RELEASE_1, DISCOGS_RELEASE),
        )
        for mb_release_id, discogs_release_id in cases:
            with self.subTest(
                mb_release_id=mb_release_id,
                discogs_release_id=discogs_release_id,
            ):
                db = FakePipelineDB()
                db.seed_request(make_request_row(
                    id=1,
                    status="wanted",
                    mb_release_id=mb_release_id,
                    discogs_release_id=discogs_release_id,
                ))
                batches: list[list[str]] = []

                with self.assertRaisesRegex(ValueError, "request 1"):
                    list_long_tail(
                        db,
                        _recording_empty_band_fn(batches),
                    )
                self.assertEqual(batches, [])

    def test_missing_when_no_beets_album(self) -> None:
        """AE1: a wanted request whose release isn't in the library bands
        Missing; an imported request is absent from the result."""
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=1, status="wanted", mb_release_id=MB_RELEASE_1,
            artist_name="A", album_title="Album"))
        db.seed_request(make_request_row(
            id=2, status="imported", mb_release_id=MB_RELEASE_2))
        # Band fn explicitly reports not in library → Missing.
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
            id=1, status="wanted", mb_release_id=MB_RELEASE_1))
        result = list_long_tail(
            db, _fixed_band_fn({MB_RELEASE_1: "transparent"}))
        self.assertEqual(result.rows[0].band, "transparent")

    def test_present_but_rank_unknown_bands_unknown(self) -> None:
        """In-library-but-unclassifiable bands Unknown, not Missing.

        The band_fn returns ``"unknown"`` for a unique current release whose
        item snapshot can't be ranked, distinct from an explicit
        ``CurrentBeetsMissing`` answer."""
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=1, status="wanted", mb_release_id=MB_RELEASE_1))
        db.seed_request(make_request_row(
            id=2, status="wanted", mb_release_id=MB_RELEASE_2))
        result = list_long_tail(
            db, _fixed_band_fn({MB_RELEASE_1: BAND_UNKNOWN}))
        by_id = {r.id: r for r in result.rows}
        # Unique-but-rank-unknown → Unknown.
        self.assertEqual(by_id[1].band, BAND_UNKNOWN)
        # Explicit current-resolution absence → Missing.
        self.assertEqual(by_id[2].band, BAND_MISSING)

    def test_track_count_and_spectral_project_onto_row(self) -> None:
        """The card meta (N tracks) + on-disk spectral strip read straight
        off the cohort projection — ``track_count`` counts ``album_tracks``;
        ``current_spectral_grade`` / ``current_spectral_bitrate`` mirror the
        denormalised request columns. NULL spectral stays NULL ("if known").
        """
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=1, status="wanted", mb_release_id=MB_RELEASE_1,
            current_spectral_grade="genuine", current_spectral_bitrate=952))
        db.set_tracks(1, [
            {"track_number": 1, "title": "A"},
            {"track_number": 2, "title": "B"},
            {"track_number": 3, "title": "C"},
        ])
        # A second wanted row with no tracks + unknown spectral.
        db.seed_request(make_request_row(
            id=2, status="wanted", mb_release_id=MB_RELEASE_2))
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
            id=1, status="wanted", mb_release_id=MB_RELEASE_1,
            mb_release_group_id="rg-1"))
        db.seed_request(make_request_row(
            id=2, status="wanted", mb_release_id=MB_RELEASE_2))
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
            id=1, status="wanted", mb_release_id=MB_RELEASE_1))
        db.seed_request(make_request_row(
            id=2, status="wanted", mb_release_id=MB_RELEASE_2))
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
            id=1, status="wanted", mb_release_id=MB_RELEASE_1))
        db.seed_request(make_request_row(
            id=2, status="wanted", mb_release_id=MB_RELEASE_2))
        db.seed_request(make_request_row(
            id=3, status="wanted", mb_release_id=_mb_release(3)))
        band_fn = _fixed_band_fn({
            MB_RELEASE_2: "transparent", _mb_release(3): "transparent",
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
            id=7, status="wanted", mb_release_id=_mb_release(7)))
        row = band_one_long_tail(
            db, _fixed_band_fn({_mb_release(7): "good"}), 7)
        assert row is not None
        self.assertIsInstance(row, LongTailRow)
        self.assertEqual(row.id, 7)
        self.assertEqual(row.band, "good")

    def test_single_id_missing_or_not_wanted_returns_none(self) -> None:
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=8, status="imported", mb_release_id=_mb_release(8)))
        self.assertIsNone(band_one_long_tail(db, _fixed_band_fn({}), 8))
        self.assertIsNone(band_one_long_tail(db, _fixed_band_fn({}), 999))


class TestListLongTailN1Guard(unittest.TestCase):
    """The cohort path's total query count is constant regardless of
    cohort size: one Postgres cohort query plus one coherent Beets resolver
    batch (modelled on ``TestListTriageN1Guard``)."""

    def test_query_count_constant_across_cohort_size(self) -> None:
        db = FakePipelineDB()
        band_calls: list[list[str]] = []

        def counting_band_fn(release_ids: list[str]) -> dict[str, str]:
            # The real band_fn issues one coherent exact-resolution batch.
            # Record the collaborator call and assert it fires once for the
            # whole cohort.
            band_calls.append(list(release_ids))
            return {rid: "transparent" for rid in release_ids}

        for i in range(1, 51):
            db.seed_request(make_request_row(
                id=i, status="wanted", mb_release_id=_mb_release(i),
                artist_name=f"Artist {i}", album_title="Album"))

        result = list_long_tail(db, counting_band_fn)
        self.assertEqual(len(result.rows), 50)
        # Exactly one Postgres cohort query.
        self.assertEqual(db.query_counts.get("get_long_tail_cohort"), 1)
        self.assertEqual(sum(db.query_counts.values()), 1)
        # Exactly one batched band call for the whole cohort — never per row.
        # This stands in for the exact resolver batch over every strict MB or
        # Discogs request identity.
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
        delete_all_rows(self.db, REQUEST_CASCADE_RESET_TABLES)

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
