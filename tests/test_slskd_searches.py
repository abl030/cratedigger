"""Tests for lib/slskd_searches.py — the write-ahead-ledger sweep
(issue #576).

Invariants under test (``.claude/rules/code-quality.md`` Red/Green TDD):

* **I1 (no leak, kill-proof)** — ``TestConvergeSlskdSearchesI1Pin``: a
  search whose creator died right after the POST (the ledger row was
  written, but ``delete()`` never ran) is still reaped by a LATER
  cycle's sweep.
* **I2 (write-ahead)** — enforced at the creation sites; pinned in
  ``TestSubmitPlanSearchWriteAheadOrdering`` /
  ``TestSearchForAlbumWriteAheadOrdering`` below (order: ledger insert
  before the POST) and, for the artist-probe site, in
  ``tests/test_unfindable_detection_service.py::TestRunArtistProbe``.
* **I3 (good-citizen, #571 doctrine)** — ``TestConvergeSlskdSearchesI3Pin``:
  a foreign (unledgered) search is NEVER touched by the sweep, in any
  state, at any age.

The generated properties patrolling the world space around these pins
live in ``tests/test_search_ledger_generated.py``.
"""
from __future__ import annotations

import configparser
import unittest
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from typing import Any

import cratedigger
from lib.config import CratediggerConfig
from lib.context import CratediggerContext
from lib.slskd_searches import (
    SEARCH_LEDGER_PRUNE_RETENTION_DAYS,
    SEARCH_LEDGER_SWEEP_GRACE_S,
    SearchSweepSummary,
    converge_slskd_searches,
)
from tests.fakes import FakePipelineDB, FakePipelineDBSource, FakeSlskdAPI
from tests.helpers import make_requests_http_error


def _cfg(**overrides: Any) -> CratediggerConfig:
    cfg = CratediggerConfig.from_ini(configparser.ConfigParser())
    if overrides:
        cfg = replace(cfg, **overrides)
    return cfg


def _ctx(db: FakePipelineDB, slskd: FakeSlskdAPI) -> CratediggerContext:
    return CratediggerContext(
        cfg=_cfg(), slskd=slskd,
        pipeline_db_source=FakePipelineDBSource(db),
    )


def _ledger(db: FakePipelineDB, search_id: str, *, purpose: str = "plan_search",
           request_id: int | None = 1, age_s: float = 0.0) -> None:
    """Record a ledger row and backdate its created_at by ``age_s``."""
    db.record_search_id(search_id, purpose, request_id)
    db._search_ledger[search_id].created_at = (
        datetime.now(timezone.utc) - timedelta(seconds=age_s))


_PAST_GRACE = SEARCH_LEDGER_SWEEP_GRACE_S + 60.0
_INSIDE_GRACE = SEARCH_LEDGER_SWEEP_GRACE_S / 2.0


class TestConvergeSlskdSearchesI1Pin(unittest.TestCase):
    """I1 (kill-proof): a ledgered search whose creating process died
    right after the POST is still reaped once past GRACE."""

    def test_kill_after_post_is_reaped_next_cycle(self):
        db = FakePipelineDB()
        slskd = FakeSlskdAPI()
        # The ledger write happened (write-ahead) and the POST reached
        # slskd, but the process died before it ever called delete() —
        # the L0 SIGTERM-mid-cycle leak, verified live on doc2.
        _ledger(db, "killed-1", request_id=7, age_s=_PAST_GRACE)
        slskd.searches.add_search(
            search_id="killed-1", state="Completed, TimedOut", responses=[])

        summary = converge_slskd_searches(_ctx(db, slskd))

        self.assertEqual(summary, SearchSweepSummary(deleted=1))
        self.assertIn("killed-1", slskd.searches.delete_calls)
        self.assertNotIn(
            "killed-1", [s["id"] for s in slskd.searches.get_all()])
        self.assertIsNotNone(db._search_ledger["killed-1"].deleted_at)

    def test_already_gone_search_is_marked_swept(self):
        """The fast-path delete in execute_search's finally already
        worked — the sweep still marks the ledger row swept so it drops
        out of future scans and eventually gets pruned."""
        db = FakePipelineDB()
        slskd = FakeSlskdAPI()
        _ledger(db, "already-gone-1", age_s=_PAST_GRACE)
        # No corresponding slskd.searches.add_search — genuinely absent.

        summary = converge_slskd_searches(_ctx(db, slskd))

        self.assertEqual(summary, SearchSweepSummary(already_gone=1))
        self.assertIsNotNone(db._search_ledger["already-gone-1"].deleted_at)

    def test_in_flight_ledgered_search_is_left_for_next_cycle(self):
        """A ledgered search still InProgress/Queued past GRACE must NOT
        be stopped or deleted — it completes on its own within ~a
        minute; a later cycle's sweep catches it once settled."""
        db = FakePipelineDB()
        slskd = FakeSlskdAPI()
        _ledger(db, "inflight-1", age_s=_PAST_GRACE)
        slskd.searches.add_search(
            search_id="inflight-1", state="InProgress", responses=[])

        summary = converge_slskd_searches(_ctx(db, slskd))

        self.assertEqual(summary, SearchSweepSummary())
        self.assertEqual(slskd.searches.delete_calls, [])
        self.assertEqual(slskd.searches.stop_calls, [])
        self.assertIsNone(db._search_ledger["inflight-1"].deleted_at)

    def test_inside_grace_window_is_left_alone(self):
        """A search ledgered moments ago (current/previous cycle) is not
        yet eligible — GRACE gives the operator an inspection window and
        avoids racing a search that's still genuinely in flight."""
        db = FakePipelineDB()
        slskd = FakeSlskdAPI()
        _ledger(db, "fresh-1", age_s=_INSIDE_GRACE)
        slskd.searches.add_search(
            search_id="fresh-1", state="Completed, TimedOut", responses=[])

        summary = converge_slskd_searches(_ctx(db, slskd))

        self.assertEqual(summary, SearchSweepSummary())
        self.assertEqual(slskd.searches.delete_calls, [])
        self.assertIsNone(db._search_ledger["fresh-1"].deleted_at)

    def test_per_id_delete_failure_does_not_block_the_rest(self):
        db = FakePipelineDB()
        slskd = FakeSlskdAPI()
        _ledger(db, "boom-1", age_s=_PAST_GRACE)
        _ledger(db, "ok-1", age_s=_PAST_GRACE)
        slskd.searches.add_search(
            search_id="boom-1", state="Completed, TimedOut", responses=[])
        slskd.searches.add_search(
            search_id="ok-1", state="Completed, TimedOut", responses=[])

        real_delete = slskd.searches.delete

        def flaky_delete(search_id: Any) -> None:
            if search_id == "boom-1":
                raise RuntimeError("slskd delete failed")
            real_delete(search_id)

        slskd.searches.delete = flaky_delete

        summary = converge_slskd_searches(_ctx(db, slskd))

        self.assertEqual(summary.deleted, 1)
        self.assertIsNone(db._search_ledger["boom-1"].deleted_at)
        self.assertIsNotNone(db._search_ledger["ok-1"].deleted_at)

    def test_get_all_failure_skips_reconciliation_but_prune_still_runs(self):
        db = FakePipelineDB()
        slskd = FakeSlskdAPI()
        _ledger(db, "unswept-1", age_s=_PAST_GRACE)

        def boom() -> list[dict[str, Any]]:
            raise RuntimeError("slskd unreachable")

        slskd.searches.get_all = boom
        # A row confirmed swept long ago, past the prune retention window.
        db.record_search_id("old-swept", "plan_search", 1)
        db.mark_search_ids_deleted(["old-swept"])
        db._search_ledger["old-swept"].deleted_at = (
            datetime.now(timezone.utc)
            - timedelta(days=SEARCH_LEDGER_PRUNE_RETENTION_DAYS + 1))

        summary = converge_slskd_searches(_ctx(db, slskd))

        self.assertEqual(summary, SearchSweepSummary())
        self.assertIsNone(db._search_ledger["unswept-1"].deleted_at)
        self.assertNotIn("old-swept", db._search_ledger)

    def test_prune_removes_old_swept_rows(self):
        db = FakePipelineDB()
        slskd = FakeSlskdAPI()
        db.record_search_id("swept-old", "plan_search", 1)
        db.mark_search_ids_deleted(["swept-old"])
        db._search_ledger["swept-old"].deleted_at = (
            datetime.now(timezone.utc)
            - timedelta(days=SEARCH_LEDGER_PRUNE_RETENTION_DAYS + 1))

        converge_slskd_searches(_ctx(db, slskd))

        self.assertNotIn("swept-old", db._search_ledger)

    def test_no_ledgered_rows_is_a_quiet_noop(self):
        db = FakePipelineDB()
        slskd = FakeSlskdAPI()
        slskd.searches.add_search(
            search_id="untouched", state="Completed, TimedOut", responses=[])

        summary = converge_slskd_searches(_ctx(db, slskd))

        self.assertEqual(summary, SearchSweepSummary())
        self.assertEqual(slskd.searches.delete_calls, [])


class TestConvergeSlskdSearchesI3Pin(unittest.TestCase):
    """I3 (good-citizen, #571 doctrine): a foreign (unledgered) search is
    never touched by the sweep, whatever its state or age."""

    def test_foreign_completed_search_survives(self):
        db = FakePipelineDB()
        slskd = FakeSlskdAPI()
        # A search cratedigger did NOT create — e.g. a human sharing the
        # instance via the slskd web UI.
        slskd.searches.add_search(
            search_id="human-1", state="Completed, ResponseLimitReached",
            responses=[])

        summary = converge_slskd_searches(_ctx(db, slskd))

        # Empty ledger short-circuits before the GET /searches fetch, so
        # there is no foreign accounting here (see
        # test_no_ledgered_rows_is_a_quiet_noop); foreign_skipped counting
        # is exercised by test_foreign_search_survives_alongside_ledgered_
        # cleanup. What this pin protects is survival: no delete, no stop.
        self.assertEqual(summary, SearchSweepSummary())
        self.assertEqual(slskd.searches.delete_calls, [])
        self.assertEqual(slskd.searches.stop_calls, [])
        self.assertIn(
            "human-1", [s["id"] for s in slskd.searches.get_all()])

    def test_foreign_in_flight_search_survives(self):
        db = FakePipelineDB()
        slskd = FakeSlskdAPI()
        slskd.searches.add_search(search_id="human-2", state="InProgress", responses=[])

        summary = converge_slskd_searches(_ctx(db, slskd))

        self.assertEqual(summary, SearchSweepSummary())
        self.assertEqual(slskd.searches.delete_calls, [])
        self.assertEqual(slskd.searches.stop_calls, [])

    def test_foreign_search_survives_alongside_ledgered_cleanup(self):
        """The sweep must discriminate correctly, not just "do nothing
        when anything foreign is present" — a ledgered search is still
        cleaned up while a co-resident foreign search is untouched."""
        db = FakePipelineDB()
        slskd = FakeSlskdAPI()
        _ledger(db, "mine-1", age_s=_PAST_GRACE)
        slskd.searches.add_search(
            search_id="mine-1", state="Completed, TimedOut", responses=[])
        slskd.searches.add_search(
            search_id="human-3", state="Completed, TimedOut", responses=[])

        summary = converge_slskd_searches(_ctx(db, slskd))

        self.assertEqual(summary, SearchSweepSummary(deleted=1, foreign_skipped=1))
        self.assertEqual(slskd.searches.delete_calls, ["mine-1"])
        self.assertIn("human-3", [s["id"] for s in slskd.searches.get_all()])

    def test_own_inside_grace_search_is_not_counted_foreign(self):
        """The good-citizen count means "a human's searches I left alone"
        — cratedigger's own not-yet-eligible (inside-grace) resident
        search must be neither deleted nor miscounted as foreign."""
        db = FakePipelineDB()
        slskd = FakeSlskdAPI()
        _ledger(db, "mine-old", age_s=_PAST_GRACE)
        _ledger(db, "mine-fresh", age_s=_INSIDE_GRACE)
        slskd.searches.add_search(
            search_id="mine-old", state="Completed, TimedOut", responses=[])
        slskd.searches.add_search(
            search_id="mine-fresh", state="Completed, TimedOut", responses=[])
        slskd.searches.add_search(
            search_id="human-4", state="Completed, TimedOut", responses=[])

        summary = converge_slskd_searches(_ctx(db, slskd))

        self.assertEqual(summary, SearchSweepSummary(deleted=1, foreign_skipped=1))
        self.assertEqual(slskd.searches.delete_calls, ["mine-old"])
        self.assertIsNone(db._search_ledger["mine-fresh"].deleted_at)

    def test_ledgered_id_matches_case_insensitively(self):
        """We mint lowercase UUIDs but nothing may depend on slskd echoing
        the same casing: an upper-cased echo must still match the ledger
        (a miss would mark the row already-gone — irreversible — while
        the real search leaked forever as \"foreign\")."""
        db = FakePipelineDB()
        slskd = FakeSlskdAPI()
        ledgered = "7b0e8d9a-1234-4f00-9a9a-abcdefabcdef"
        _ledger(db, ledgered, age_s=_PAST_GRACE)
        slskd.searches.add_search(
            search_id=ledgered.upper(), state="Completed, TimedOut",
            responses=[])

        summary = converge_slskd_searches(_ctx(db, slskd))

        self.assertEqual(summary, SearchSweepSummary(deleted=1))
        self.assertEqual(slskd.searches.delete_calls, [ledgered.upper()])
        self.assertIsNotNone(db._search_ledger[ledgered].deleted_at)


class TestSubmitPlanSearchWriteAheadOrdering(unittest.TestCase):
    """I2 pin: cratedigger._submit_plan_search ledgers the id BEFORE
    issuing the POST — order-recording fake asserts the ledger call
    precedes search_text for the SAME id."""

    def test_ledger_insert_precedes_the_post(self):
        from album_source import AlbumRecord, MediaRecord, ReleaseRecord

        order: list[str] = []
        db = FakePipelineDB()
        slskd = FakeSlskdAPI()

        real_record = db.record_search_id

        def recording_record_search_id(search_id, purpose, request_id):
            order.append(f"ledger:{search_id}")
            return real_record(search_id, purpose, request_id)

        db.record_search_id = recording_record_search_id

        real_search_text = slskd.searches.search_text

        def recording_search_text(**kwargs):
            result = real_search_text(**kwargs)
            order.append(f"post:{result['id']}")
            return result

        slskd.searches.search_text = recording_search_text

        media = [MediaRecord(medium_number=1, medium_format="CD", track_count=1)]
        release = ReleaseRecord(
            id=-42, foreign_release_id="mbid", title="Album", track_count=1,
            medium_count=1, format="CD", media=media, monitored=True,
            country=["US"], status="Official",
        )
        album = AlbumRecord(
            id=-42, title="Album", release_date="1999-01-01T00:00:00Z",
            artist_id=0, artist_name="Artist", foreign_artist_id="",
            releases=[release], db_request_id=42, db_source="request",
            db_mb_release_id="mbid", db_search_filetype_override=None,
            db_target_format=None,
        )

        result = cratedigger._submit_plan_search(
            album, "Artist Album", "default", _cfg(), slskd, db)

        assert result is not None
        search_id = result[0]
        self.assertEqual(order, [f"ledger:{search_id}", f"post:{search_id}"])
        self.assertEqual(db.record_search_id_calls[0].request_id, 42)
        self.assertEqual(db.record_search_id_calls[0].purpose, "plan_search")

    def test_each_retry_attempt_ledgers_its_own_fresh_id(self):
        """A retried attempt is not a resubmission of the same id — each
        429/409 retry mints + ledgers a fresh id so a half-created
        earlier attempt (POST reached slskd, response lost) is still
        sweepable on its own ledger row."""
        from album_source import AlbumRecord, MediaRecord, ReleaseRecord

        db = FakePipelineDB()
        slskd = FakeSlskdAPI()
        attempts: list[Any] = []

        def flaky_search_text(**kwargs):
            attempts.append(kwargs["id"])
            if len(attempts) < 3:
                raise make_requests_http_error("busy", status_code=409)
            return {"id": kwargs["id"]}

        slskd.searches.search_text = flaky_search_text

        media = [MediaRecord(medium_number=1, medium_format="CD", track_count=1)]
        release = ReleaseRecord(
            id=-1, foreign_release_id="mbid", title="Album", track_count=1,
            medium_count=1, format="CD", media=media, monitored=True,
            country=["US"], status="Official",
        )
        album = AlbumRecord(
            id=-1, title="Album", release_date="1999-01-01T00:00:00Z",
            artist_id=0, artist_name="Artist", foreign_artist_id="",
            releases=[release], db_request_id=1, db_source="request",
            db_mb_release_id="mbid", db_search_filetype_override=None,
            db_target_format=None,
        )

        import time as _time
        real_sleep = _time.sleep
        _time.sleep = lambda _s: None
        try:
            result = cratedigger._submit_plan_search(
                album, "Artist Album", "default", _cfg(), slskd, db)
        finally:
            _time.sleep = real_sleep

        assert result is not None
        # Three attempts, three DISTINCT minted ids, all three ledgered.
        self.assertEqual(len(attempts), 3)
        self.assertEqual(len(set(attempts)), 3)
        self.assertEqual(len(db.record_search_id_calls), 3)
        ledgered_ids = {c.search_id for c in db.record_search_id_calls}
        self.assertEqual(ledgered_ids, set(attempts))
        # The successful (final) attempt's id is what's returned.
        self.assertEqual(result[0], attempts[-1])


class TestSearchForAlbumWriteAheadOrdering(unittest.TestCase):
    """I2 pin for the serial fallback path: search_for_album ledgers the
    id BEFORE calling execute_search's submit."""

    def test_ledger_insert_precedes_the_post(self):
        from album_source import AlbumRecord, MediaRecord, ReleaseRecord
        from lib.pipeline_db import SearchPlanItemInput
        from lib.search import SEARCH_PLAN_GENERATOR_ID

        order: list[str] = []
        db = FakePipelineDB()
        slskd = FakeSlskdAPI()
        rid = db.add_request(
            artist_name="Artist", album_title="Album",
            source="request", mb_release_id="mbid-test",
        )
        db.create_successful_search_plan(
            request_id=rid, generator_id=SEARCH_PLAN_GENERATOR_ID,
            items=[SearchPlanItemInput(
                ordinal=0, strategy="default", query="Artist Album",
                canonical_query_key="artist album")],
        )

        real_record = db.record_search_id

        def recording_record_search_id(search_id, purpose, request_id):
            order.append(f"ledger:{search_id}")
            return real_record(search_id, purpose, request_id)

        db.record_search_id = recording_record_search_id

        real_search_text = slskd.searches.search_text

        def recording_search_text(**kwargs):
            result = real_search_text(**kwargs)
            order.append(f"post:{result['id']}")
            return result

        slskd.searches.search_text = recording_search_text

        media = [MediaRecord(medium_number=1, medium_format="CD", track_count=1)]
        release = ReleaseRecord(
            id=-rid, foreign_release_id="mbid-test", title="Album",
            track_count=1, medium_count=1, format="CD", media=media,
            monitored=True, country=["US"], status="Official",
        )
        album = AlbumRecord(
            id=-rid, title="Album", release_date="1999-01-01T00:00:00Z",
            artist_id=0, artist_name="Artist", foreign_artist_id="",
            releases=[release], db_request_id=rid, db_source="request",
            db_mb_release_id="mbid-test", db_search_filetype_override=None,
            db_target_format=None,
        )
        ctx = _ctx(db, slskd)

        orig_cfg, orig_slskd, orig_pdb, orig_module_ctx = (
            cratedigger.cfg, cratedigger.slskd,
            cratedigger.pipeline_db_source, cratedigger._module_ctx,
        )
        cratedigger.cfg = ctx.cfg
        cratedigger.slskd = slskd
        cratedigger.pipeline_db_source = ctx.pipeline_db_source
        cratedigger._module_ctx = ctx
        try:
            result = cratedigger.search_for_album(album, ctx)
        finally:
            cratedigger.cfg, cratedigger.slskd = orig_cfg, orig_slskd
            cratedigger.pipeline_db_source = orig_pdb
            cratedigger._module_ctx = orig_module_ctx

        self.assertTrue(order and order[0].startswith("ledger:"))
        search_id = order[0].split(":", 1)[1]
        self.assertEqual(order, [f"ledger:{search_id}", f"post:{search_id}"])
        self.assertEqual(result.query, "Artist Album")


if __name__ == "__main__":
    unittest.main()
