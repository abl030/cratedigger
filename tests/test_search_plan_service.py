"""Tests for `lib.search_plan_service.SearchPlanService`.

Covers AE1, AE2, AE12, AE13 from the persisted-search-plans plan plus the
edge cases enumerated in §U3:

* add-time generation creates an active plan + cursor at ordinal 0
* deterministic no-runnable-query failure is sticky and does not make the
  request searchable
* resolver outage records a transient failure that a later call can clear
* CLI and web add paths converge on the same `ReleaseSnapshot` /
  `SearchPlan` for the same release data
* duplicate-add and explicit-regenerate semantics
* failure preserves the previously-active successful plan
* sanitizer redacts paths / secrets / truncates long blobs
* generator-id is the single source of truth across CLI / web / service.
"""

from __future__ import annotations

import os
import sys
import unittest
from unittest.mock import MagicMock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from lib.config import CratediggerConfig
from lib.pipeline_db import (
    ADVISORY_LOCK_NAMESPACE_PLAN,
    PLAN_STATUS_ACTIVE,
    PLAN_STATUS_FAILED_DETERMINISTIC,
    PLAN_STATUS_FAILED_TRANSIENT,
    PLAN_STATUS_SUPERSEDED,
)
from lib.release_snapshot import (
    ResolverFailure,
    ResolverMetadataIncomplete,
    snapshot_from_add_payload,
    snapshot_from_request_row,
)
from lib.search import SEARCH_PLAN_GENERATOR_ID, generate_search_plan
from lib.search_plan_service import (
    FAILURE_CLASS_DEPENDENCY_FAILURE,
    FAILURE_CLASS_METADATA_INCOMPLETE,
    FAILURE_CLASS_NO_RUNNABLE_QUERY,
    FAILURE_CLASS_RESOLVER_UNAVAILABLE,
    MAX_ERROR_MESSAGE_BYTES,
    RESULT_FAILED_DETERMINISTIC,
    RESULT_FAILED_TRANSIENT,
    RESULT_NOOP_ACTIVE_PLAN_EXISTS,
    RESULT_REQUEST_NOT_FOUND,
    RESULT_SUCCESS,
    SearchPlanService,
    sanitize_error_message,
    sanitize_provenance,
    search_plan_config_from_cratedigger_config,
)
from tests.fakes import FakePipelineDB
from tests.helpers import make_request_row


def _seed_request(db: FakePipelineDB, **overrides):
    row = make_request_row(**overrides)
    db.seed_request(row)
    return row


def _ok_tracks() -> list[dict[str, object]]:
    return [
        {"disc_number": 1, "track_number": 1, "title": "Sister Night"},
        {"disc_number": 1, "track_number": 2, "title": "American Hero Story"},
        {"disc_number": 1, "track_number": 3, "title": "Martial Feats Of Comanche Horsemanship"},
        {"disc_number": 1, "track_number": 4, "title": "Pangloss"},
    ]


class TestSearchPlanServiceAddTime(unittest.TestCase):
    """AE1 + duplicate-add + empty-tracks edges for the add-time path."""

    def setUp(self):
        self.db = FakePipelineDB()
        self.cfg = CratediggerConfig()
        self.svc = SearchPlanService(self.db, self.cfg)

    def test_add_time_generation_creates_active_plan_and_initialises_cursor(self):
        """AE1: add-time generation after tracks are persisted creates an
        active plan and initialises the cursor at ordinal 0."""
        _seed_request(self.db, id=1, artist_name="Trent Reznor", album_title="Watchmen", year=2019)
        tracks = _ok_tracks()
        self.db.set_tracks(1, tracks)

        result = self.svc.generate_for_new_request(
            1,
            artist_name="Trent Reznor",
            album_title="Watchmen",
            year=2019,
            tracks=tracks,
            source="request",
        )

        self.assertEqual(result.outcome, RESULT_SUCCESS)
        self.assertIsNotNone(result.plan_id)
        active = self.db.get_active_search_plan(1)
        self.assertIsNotNone(active)
        assert active is not None  # narrowing for pyright
        self.assertEqual(active.plan.status, PLAN_STATUS_ACTIVE)
        self.assertEqual(active.plan.generator_id, SEARCH_PLAN_GENERATOR_ID)
        self.assertEqual(active.next_ordinal, 0)
        self.assertEqual(active.cycle_count, 0)
        self.assertGreater(len(active.items), 0)

    def test_empty_tracklist_produces_album_level_only_plan(self):
        """A zero-track release still has runnable album-level queries."""
        _seed_request(self.db, id=2, artist_name="Tycho", album_title="Awake", year=2014)

        result = self.svc.generate_for_new_request(
            2,
            artist_name="Tycho",
            album_title="Awake",
            year=2014,
            tracks=[],
            source="request",
        )

        self.assertEqual(result.outcome, RESULT_SUCCESS)
        active = self.db.get_active_search_plan(2)
        assert active is not None
        strategies = {it.strategy for it in active.items}
        # No track_<n> slots when there are no tracks; album-level only.
        self.assertFalse(any(s.startswith("track_") for s in strategies))
        self.assertIn("default", strategies)

    def test_config_album_prepend_artist_is_default_for_add_time_generation(self):
        db = FakePipelineDB()
        svc = SearchPlanService(
            db, CratediggerConfig(album_prepend_artist=True))
        _seed_request(
            db, id=4, artist_name="Tycho", album_title="Awake", year=2014)

        result = svc.generate_for_new_request(
            4,
            artist_name="Tycho",
            album_title="Awake",
            year=2014,
            tracks=[],
            source="request",
        )

        self.assertEqual(result.outcome, RESULT_SUCCESS)
        active = db.get_active_search_plan(4)
        assert active is not None
        self.assertEqual(active.plan.metadata_snapshot.prepend_artist, True)
        self.assertEqual(active.items[0].query, "*ycho Awake")

    def test_add_time_failure_records_failed_deterministic_plan(self):
        """AE2: deterministic no-runnable-query failure stays wanted/not-searchable."""
        _seed_request(self.db, id=3, artist_name="", album_title="", year=None)

        result = self.svc.generate_for_new_request(
            3,
            artist_name="",
            album_title="",
            year=None,
            tracks=[],
            source="request",
        )

        self.assertEqual(result.outcome, RESULT_FAILED_DETERMINISTIC)
        self.assertEqual(result.failure_class, FAILURE_CLASS_NO_RUNNABLE_QUERY)
        # No active plan, but a failed_deterministic row exists.
        self.assertIsNone(self.db.get_active_search_plan(3))
        statuses = {p.status for p in self.db.search_plans.values()
                    if p.request_id == 3}
        self.assertIn(PLAN_STATUS_FAILED_DETERMINISTIC, statuses)
        # Request still wanted (add path is repairable).
        self.assertEqual(self.db.request(3)["status"], "wanted")


class TestSearchPlanServiceRegenerate(unittest.TestCase):
    """`generate_for_request` paths: no-op, regenerate, repair."""

    def setUp(self):
        self.db = FakePipelineDB()
        self.cfg = CratediggerConfig()
        self.svc = SearchPlanService(self.db, self.cfg)

    def _seed_with_active_plan(self, request_id: int = 10) -> int:
        _seed_request(self.db, id=request_id,
                       artist_name="Phoebe Bridgers", album_title="Punisher",
                       year=2020)
        self.db.set_tracks(request_id, _ok_tracks())
        result = self.svc.generate_for_new_request(
            request_id,
            artist_name="Phoebe Bridgers",
            album_title="Punisher",
            year=2020,
            tracks=_ok_tracks(),
            source="request",
        )
        self.assertEqual(result.outcome, RESULT_SUCCESS)
        assert result.plan_id is not None  # narrowing for pyright
        return result.plan_id

    def test_no_op_when_active_plan_already_exists(self):
        plan_id = self._seed_with_active_plan(10)
        self.db.advisory_lock_calls.clear()
        again = self.svc.generate_for_request(10, regenerate=False)
        self.assertEqual(again.outcome, RESULT_NOOP_ACTIVE_PLAN_EXISTS)
        self.assertEqual(again.plan_id, plan_id)
        self.assertEqual(
            self.db.advisory_lock_calls,
            [(ADVISORY_LOCK_NAMESPACE_PLAN, 10)],
        )
        # No second active plan.
        active = self.db.get_active_search_plan(10)
        assert active is not None
        self.assertEqual(active.plan.id, plan_id)

    def test_rereads_active_plan_after_acquiring_lock(self):
        _seed_request(
            self.db, id=14,
            artist_name="Phoebe Bridgers", album_title="Punisher", year=2020)
        self.db.set_tracks(14, _ok_tracks())

        def acquire_and_race(namespace: int, key: int) -> bool:
            if namespace == ADVISORY_LOCK_NAMESPACE_PLAN and key == 14:
                from lib.pipeline_db import SearchPlanItemInput
                self.db.create_successful_search_plan(
                    request_id=14,
                    generator_id=SEARCH_PLAN_GENERATOR_ID,
                    items=[SearchPlanItemInput(
                        ordinal=0, strategy="default", query="q")],
                )
            return True

        self.db.set_advisory_lock_result(acquire_and_race)
        result = self.svc.generate_for_request(14, regenerate=False)

        self.assertEqual(result.outcome, RESULT_NOOP_ACTIVE_PLAN_EXISTS)
        self.assertEqual(len(self.db.search_plans), 1)

    def test_config_album_prepend_artist_is_default_for_request_generation(self):
        db = FakePipelineDB()
        _seed_request(
            db, id=15, artist_name="Tycho", album_title="Awake", year=2014)
        db.set_tracks(15, [])
        svc = SearchPlanService(
            db, CratediggerConfig(album_prepend_artist=True))

        result = svc.generate_for_request(15, regenerate=False)

        self.assertEqual(result.outcome, RESULT_SUCCESS)
        active = db.get_active_search_plan(15)
        assert active is not None
        self.assertEqual(active.plan.metadata_snapshot.prepend_artist, True)
        self.assertEqual(active.items[0].query, "*ycho Awake")

    def test_regenerate_supersedes_previous_active_plan(self):
        """AE10-prereq: explicit regeneration replaces and preserves history."""
        old_plan_id = self._seed_with_active_plan(10)
        result = self.svc.generate_for_request(10, regenerate=True)
        self.assertEqual(result.outcome, RESULT_SUCCESS)
        self.assertTrue(result.is_supersede)
        self.assertNotEqual(result.plan_id, old_plan_id)
        active = self.db.get_active_search_plan(10)
        assert active is not None
        self.assertEqual(active.plan.id, result.plan_id)
        # Old plan flipped to superseded.
        old = self.db.search_plans[old_plan_id]
        self.assertEqual(old.status, PLAN_STATUS_SUPERSEDED)

    def test_regeneration_failure_preserves_old_active_plan(self):
        """Explicit regeneration that hits a deterministic generator
        failure must NOT supersede the existing active plan."""
        old_plan_id = self._seed_with_active_plan(10)
        # Mutate the request to make regeneration deterministically fail.
        self.db.request(10)["artist_name"] = ""
        self.db.request(10)["album_title"] = ""
        self.db._tracks[10] = []

        result = self.svc.generate_for_request(10, regenerate=True)
        self.assertEqual(result.outcome, RESULT_FAILED_DETERMINISTIC)
        # Old active plan still active.
        active = self.db.get_active_search_plan(10)
        assert active is not None
        self.assertEqual(active.plan.id, old_plan_id)
        self.assertEqual(active.plan.status, PLAN_STATUS_ACTIVE)
        # Failed row was recorded.
        latest_failed = [p for p in self.db.search_plans.values()
                         if p.request_id == 10
                         and p.status == PLAN_STATUS_FAILED_DETERMINISTIC]
        self.assertEqual(len(latest_failed), 1)

    def test_regeneration_works_for_non_wanted_status(self):
        """Imported / manual / downloading requests can still regenerate."""
        self._seed_with_active_plan(11)
        self.db.request(11)["status"] = "imported"
        result = self.svc.generate_for_request(11, regenerate=True)
        self.assertEqual(result.outcome, RESULT_SUCCESS)
        # Status itself is unchanged — regeneration does not flip status.
        self.assertEqual(self.db.request(11)["status"], "imported")

    def test_repair_path_when_request_has_tracks_but_no_plan(self):
        """Interrupted add: tracks persisted, plan never written.
        `generate_for_request` repairs without regenerate=True."""
        _seed_request(self.db, id=12,
                       artist_name="Caribou", album_title="Suddenly", year=2020)
        self.db.set_tracks(12, _ok_tracks())
        # No active plan yet.
        self.assertIsNone(self.db.get_active_search_plan(12))

        result = self.svc.generate_for_request(12, regenerate=False)
        self.assertEqual(result.outcome, RESULT_SUCCESS)
        active = self.db.get_active_search_plan(12)
        assert active is not None
        self.assertEqual(active.next_ordinal, 0)

    def test_old_generator_plan_is_replaced_on_implicit_call(self):
        """generate_for_request without regenerate=True should still
        replace plans whose generator_id != current."""
        plan_id = self._seed_with_active_plan(13)
        # Hand-edit the stored plan's generator_id to simulate a bump.
        self.db.search_plans[plan_id].generator_id = "search-plan/old-1999-1"

        result = self.svc.generate_for_request(13, regenerate=False)
        self.assertEqual(result.outcome, RESULT_SUCCESS)
        self.assertTrue(result.is_supersede)
        active = self.db.get_active_search_plan(13)
        assert active is not None
        self.assertEqual(active.plan.generator_id, SEARCH_PLAN_GENERATOR_ID)
        self.assertNotEqual(active.plan.id, plan_id)

    def test_request_not_found(self):
        result = self.svc.generate_for_request(99999, regenerate=False)
        self.assertEqual(result.outcome, RESULT_REQUEST_NOT_FOUND)


class TestSearchPlanServiceTrackCountReplan(unittest.TestCase):
    """#3: a plan generated with N tracks must regenerate when the
    request later has more tracks than the plan was built against."""

    def setUp(self):
        self.db = FakePipelineDB()
        self.cfg = CratediggerConfig()
        self.svc = SearchPlanService(self.db, self.cfg)

    def _seed_request_with_tracks(self, request_id: int, tracks: list[dict]):
        _seed_request(
            self.db, id=request_id,
            artist_name="Phoebe Bridgers", album_title="Punisher", year=2020,
        )
        self.db.set_tracks(request_id, tracks)
        result = self.svc.generate_for_new_request(
            request_id, artist_name="Phoebe Bridgers", album_title="Punisher",
            year=2020, tracks=tracks, source="request",
        )
        self.assertEqual(result.outcome, RESULT_SUCCESS)
        return result.plan_id

    def test_more_tracks_than_snapshot_forces_replan(self):
        """A plan generated with 3 tracks regenerates when the request
        is later updated to carry 12 tracks."""
        partial = _ok_tracks()[:3]
        old_plan_id = self._seed_request_with_tracks(60, partial)
        # Replace tracks with the full 4-track set (more than recorded 3).
        full = _ok_tracks()
        self.assertGreater(len(full), 3)
        self.db.set_tracks(60, full)

        result = self.svc.generate_for_request(60, regenerate=False)
        self.assertEqual(result.outcome, RESULT_SUCCESS)
        self.assertTrue(result.is_supersede)
        self.assertNotEqual(result.plan_id, old_plan_id)
        # The new plan's snapshot matches the new track count.
        active = self.db.get_active_search_plan(60)
        assert active is not None
        snap = active.plan.metadata_snapshot
        assert snap is not None
        self.assertEqual(snap.track_count, len(full))

    def test_same_track_count_no_replan(self):
        """When today's tracks count matches the snapshot, no regeneration."""
        full = _ok_tracks()
        old_plan_id = self._seed_request_with_tracks(61, full)
        before = len(self.db.search_plans)
        result = self.svc.generate_for_request(61, regenerate=False)
        self.assertEqual(result.outcome, RESULT_NOOP_ACTIVE_PLAN_EXISTS)
        self.assertEqual(result.plan_id, old_plan_id)
        self.assertEqual(len(self.db.search_plans), before)

    def test_missing_track_count_in_snapshot_skips_check(self):
        """Older plans without ``track_count`` in metadata_snapshot must
        not be replanned by the partial-track check (they are repaired
        on the next generator-id bump)."""
        old_plan_id = self._seed_request_with_tracks(62, _ok_tracks())
        assert old_plan_id is not None  # narrowing
        # Hand-edit the snapshot to drop track_count (older plan).
        plan = self.db.search_plans[old_plan_id]
        plan.metadata_snapshot = {
            k: v for k, v in (plan.metadata_snapshot or {}).items()
            if k != "track_count"
        }
        # Add many more tracks; partial-track check must NOT fire.
        big = _ok_tracks() + _ok_tracks()
        self.db.set_tracks(62, big)
        result = self.svc.generate_for_request(62, regenerate=False)
        self.assertEqual(result.outcome, RESULT_NOOP_ACTIVE_PLAN_EXISTS)


class TestSearchPlanServiceFailureStickiness(unittest.TestCase):
    """`failed_deterministic` is sticky for the current generator id; a
    `failed_transient` plan is sticky for a configurable retry window
    before another attempt is allowed."""

    def setUp(self):
        self.db = FakePipelineDB()
        self.cfg = CratediggerConfig()
        self.svc = SearchPlanService(self.db, self.cfg)

    def _seed_unrunnable(self, request_id: int = 50) -> None:
        """Seed a request whose metadata cannot produce any runnable query."""
        _seed_request(
            self.db, id=request_id,
            artist_name="", album_title="", year=None,
        )

    def test_deterministic_failure_does_not_create_a_second_row(self):
        """Calling generate_for_request twice on a request whose latest
        attempt is failed_deterministic must NOT insert a new row."""
        self._seed_unrunnable(50)
        first = self.svc.generate_for_request(50, regenerate=False)
        self.assertEqual(first.outcome, RESULT_FAILED_DETERMINISTIC)
        before = len(self.db.search_plans)
        # Second call: previous failure must short-circuit.
        second = self.svc.generate_for_request(50, regenerate=False)
        self.assertEqual(second.outcome, RESULT_FAILED_DETERMINISTIC)
        self.assertEqual(second.plan_id, first.plan_id)
        self.assertEqual(len(self.db.search_plans), before)

    def test_transient_failure_short_circuits_within_retry_window(self):
        """A recent transient failure must short-circuit additional
        generate_for_request calls until the retry window elapses."""
        from lib.search_plan_service import _TRANSIENT_FAILURE_RETRY_INTERVAL
        _seed_request(self.db, id=51, artist_name="X", album_title="Y",
                      mb_release_id="release-uuid")

        class FlakyResolver:
            calls = 0

            def resolve_tracks(self, *, release_id: str, request_id: int):
                FlakyResolver.calls += 1
                raise ResolverFailure("MB API timed out")

        svc = SearchPlanService(self.db, self.cfg, resolver=FlakyResolver())
        first = svc.generate_for_request(51, regenerate=False)
        self.assertEqual(first.outcome, RESULT_FAILED_TRANSIENT)
        before = len(self.db.search_plans)

        # Within the retry window: short-circuit (no new row, no resolver call).
        calls_at_start = FlakyResolver.calls
        second = svc.generate_for_request(51, regenerate=False)
        self.assertEqual(second.outcome, RESULT_FAILED_TRANSIENT)
        self.assertEqual(second.plan_id, first.plan_id)
        self.assertEqual(len(self.db.search_plans), before)
        self.assertEqual(FlakyResolver.calls, calls_at_start)

        # Backdate the recorded transient failure to before the window.
        latest = max(
            (p for p in self.db.search_plans.values()
             if p.request_id == 51 and p.status == PLAN_STATUS_FAILED_TRANSIENT),
            key=lambda p: p.created_at,
        )
        from datetime import timezone
        backdated = (
            latest.created_at - _TRANSIENT_FAILURE_RETRY_INTERVAL
            - _TRANSIENT_FAILURE_RETRY_INTERVAL
        )
        if backdated.tzinfo is None:
            backdated = backdated.replace(tzinfo=timezone.utc)
        latest.created_at = backdated

        # After the window: a new attempt is permitted.
        third = svc.generate_for_request(51, regenerate=False)
        # The flaky resolver still raises, so a new failed_transient row
        # is created (different id from the first).
        self.assertEqual(third.outcome, RESULT_FAILED_TRANSIENT)
        self.assertNotEqual(third.plan_id, first.plan_id)
        self.assertEqual(len(self.db.search_plans), before + 1)

    def test_old_generator_failed_does_not_block_current_generator_attempt(self):
        """A failed_deterministic row from an old generator id must NOT
        short-circuit the current generator id."""
        self._seed_unrunnable(52)
        # Manually insert a failed_deterministic row for an old gen id.
        self.db.create_failed_search_plan(
            request_id=52,
            generator_id="search-plan/ancient-1",
            failure_class=FAILURE_CLASS_NO_RUNNABLE_QUERY,
            error_message="old failure",
            transient=False,
        )
        before = len(self.db.search_plans)
        result = self.svc.generate_for_request(52, regenerate=False)
        # Current-generator attempt runs and produces its own
        # failed_deterministic row.
        self.assertEqual(result.outcome, RESULT_FAILED_DETERMINISTIC)
        self.assertEqual(len(self.db.search_plans), before + 1)

    def test_old_generator_active_replace_failure_is_sticky(self):
        """When auto-replacing an old-generator active plan, a new
        deterministic failure under the current generator must be sticky:
        a follow-up call must not create yet another failed row."""
        # Seed a request and produce a successful active plan.
        _seed_request(
            self.db, id=53,
            artist_name="Phoebe Bridgers", album_title="Punisher", year=2020,
        )
        self.db.set_tracks(53, _ok_tracks())
        first = self.svc.generate_for_new_request(
            53, artist_name="Phoebe Bridgers", album_title="Punisher",
            year=2020, tracks=_ok_tracks(), source="request",
        )
        self.assertEqual(first.outcome, RESULT_SUCCESS)
        # Pretend it's an old-generator plan.
        plan_id = first.plan_id
        assert plan_id is not None
        self.db.search_plans[plan_id].generator_id = "search-plan/old-1"
        # Mutate the request so regeneration deterministically fails.
        self.db.request(53)["artist_name"] = ""
        self.db.request(53)["album_title"] = ""
        self.db._tracks[53] = []

        # First implicit-regenerate fails deterministically.
        a = self.svc.generate_for_request(53, regenerate=False)
        self.assertEqual(a.outcome, RESULT_FAILED_DETERMINISTIC)
        before = len(self.db.search_plans)

        # Second call must NOT add another failed row for the current gen id.
        b = self.svc.generate_for_request(53, regenerate=False)
        self.assertEqual(b.outcome, RESULT_FAILED_DETERMINISTIC)
        self.assertEqual(b.plan_id, a.plan_id)
        self.assertEqual(len(self.db.search_plans), before)

    def test_current_active_plan_wins_over_historical_failures(self):
        _seed_request(
            self.db, id=54,
            artist_name="Phoebe Bridgers", album_title="Punisher", year=2020,
        )
        self.db.set_tracks(54, _ok_tracks())
        active = self.svc.generate_for_new_request(
            54, artist_name="Phoebe Bridgers", album_title="Punisher",
            year=2020, tracks=_ok_tracks(), source="request",
        )
        self.assertEqual(active.outcome, RESULT_SUCCESS)
        failed = self.db.create_failed_search_plan(
            request_id=54,
            generator_id=SEARCH_PLAN_GENERATOR_ID,
            failure_class=FAILURE_CLASS_NO_RUNNABLE_QUERY,
            error_message="old failure",
            transient=False,
        )

        result = self.svc.generate_for_request(54, regenerate=False)

        self.assertEqual(result.outcome, RESULT_NOOP_ACTIVE_PLAN_EXISTS)
        self.assertEqual(result.plan_id, active.plan_id)
        self.assertEqual(
            self.db.search_plans[failed].status,
            PLAN_STATUS_FAILED_DETERMINISTIC,
        )


class TestSearchPlanServiceResolver(unittest.TestCase):
    """AE12 + metadata-incomplete edge: resolver outage / no metadata."""

    def setUp(self):
        self.db = FakePipelineDB()
        self.cfg = CratediggerConfig()

    def test_resolver_outage_records_transient_failure(self):
        """AE12: a resolver outage during a startup-style call records
        `failed_transient` and a later call can succeed."""
        _seed_request(self.db, id=20,
                       artist_name="Rina Sawayama", album_title="SAWAYAMA",
                       year=2020, mb_release_id="release-uuid")
        # No tracks persisted; resolver will be consulted.

        class FlakyResolver:
            def __init__(self):
                self.calls = 0

            def resolve_tracks(self, *, release_id: str, request_id: int):
                self.calls += 1
                if self.calls == 1:
                    raise ResolverFailure("MB API timed out")
                return _ok_tracks()

        resolver = FlakyResolver()
        svc = SearchPlanService(self.db, self.cfg, resolver=resolver)

        first = svc.generate_for_request(20, regenerate=False)
        self.assertEqual(first.outcome, RESULT_FAILED_TRANSIENT)
        self.assertEqual(first.failure_class,
                         FAILURE_CLASS_RESOLVER_UNAVAILABLE)
        self.assertIsNone(self.db.get_active_search_plan(20))

        # Backdate the recorded transient failure past the retry window
        # so the next call is permitted to actually run the resolver.
        from datetime import timezone
        from lib.search_plan_service import _TRANSIENT_FAILURE_RETRY_INTERVAL
        latest = max(
            (p for p in self.db.search_plans.values()
             if p.request_id == 20 and p.status == PLAN_STATUS_FAILED_TRANSIENT),
            key=lambda p: p.created_at,
        )
        backdated = (
            latest.created_at - _TRANSIENT_FAILURE_RETRY_INTERVAL
            - _TRANSIENT_FAILURE_RETRY_INTERVAL
        )
        if backdated.tzinfo is None:
            backdated = backdated.replace(tzinfo=timezone.utc)
        latest.created_at = backdated

        # Later retry succeeds.
        second = svc.generate_for_request(20, regenerate=False)
        self.assertEqual(second.outcome, RESULT_SUCCESS)
        active = self.db.get_active_search_plan(20)
        assert active is not None
        self.assertEqual(active.plan.generator_id, SEARCH_PLAN_GENERATOR_ID)

    def test_metadata_incomplete_is_deterministic(self):
        """Resolver succeeds but reports incomplete metadata → deterministic."""
        _seed_request(self.db, id=21,
                       artist_name="Anonymous", album_title="???",
                       mb_release_id="release-uuid")

        class EmptyResolver:
            def resolve_tracks(self, *, release_id: str, request_id: int):
                raise ResolverMetadataIncomplete("no usable tracks")

        svc = SearchPlanService(self.db, self.cfg, resolver=EmptyResolver())
        result = svc.generate_for_request(21, regenerate=False)
        self.assertEqual(result.outcome, RESULT_FAILED_DETERMINISTIC)
        self.assertEqual(result.failure_class,
                         FAILURE_CLASS_METADATA_INCOMPLETE)

    def test_unexpected_resolver_exception_is_transient(self):
        """A surprise exception from the resolver maps to dependency_failure."""
        _seed_request(self.db, id=22,
                       artist_name="Idles", album_title="Joy as an Act of Resistance",
                       mb_release_id="release-uuid")

        class BoomResolver:
            def resolve_tracks(self, *, release_id: str, request_id: int):
                raise RuntimeError("upstream returned 502")

        svc = SearchPlanService(self.db, self.cfg, resolver=BoomResolver())
        result = svc.generate_for_request(22, regenerate=False)
        self.assertEqual(result.outcome, RESULT_FAILED_TRANSIENT)
        self.assertEqual(result.failure_class,
                         FAILURE_CLASS_DEPENDENCY_FAILURE)


class TestSearchPlanSnapshotEquivalence(unittest.TestCase):
    """AE13: CLI and web add paths produce equivalent plans."""

    def test_cli_and_web_paths_produce_equivalent_snapshots_and_plans(self):
        """Snapshot and persisted plan items match between
        `snapshot_from_add_payload` (CLI/web add) and
        `snapshot_from_request_row` (startup/regeneration) for the same
        release data."""
        artist = "Big Thief"
        title = "Two Hands"
        year = 2019
        tracks = _ok_tracks()
        source = "request"

        # Add-payload-style snapshot.
        snap_add = snapshot_from_add_payload(
            artist_name=artist, album_title=title, year=year,
            tracks=tracks, source=source,
        )
        # Persisted-row-style snapshot.
        row = make_request_row(
            artist_name=artist, album_title=title, year=year, source=source,
        )
        snap_row = snapshot_from_request_row(row, tracks)

        self.assertEqual(snap_add, snap_row)

        # Same generator → same plan.
        from lib.search import SearchPlanConfig
        plan_a = generate_search_plan(snap_add, SearchPlanConfig())
        plan_b = generate_search_plan(snap_row, SearchPlanConfig())
        self.assertEqual(plan_a, plan_b)

    def test_cli_and_web_share_generator_id_and_config_source(self):
        """Single source of truth for generator-id and SearchPlanConfig."""
        cfg = CratediggerConfig()
        from lib.search_plan_service import (
            SearchPlanService,
            search_plan_config_from_cratedigger_config,
        )

        svc = SearchPlanService(FakePipelineDB(), cfg)
        self.assertEqual(svc.generator_id, SEARCH_PLAN_GENERATOR_ID)
        plan_cfg = search_plan_config_from_cratedigger_config(cfg)
        self.assertEqual(plan_cfg.escalation_threshold,
                         cfg.search_escalation_threshold)


class TestSearchPlanServiceSanitizer(unittest.TestCase):
    """Sanitizer guards persisted error/provenance against length + secrets."""

    def test_truncates_oversize_error_to_cap_with_marker(self):
        # Whitespace-broken filler that won't match the secret-shape regex.
        chunk = "lorem ipsum dolor sit amet "
        long = chunk * ((MAX_ERROR_MESSAGE_BYTES // len(chunk)) + 5)
        out = sanitize_error_message(long)
        assert out is not None
        self.assertLessEqual(len(out.encode("utf-8")), MAX_ERROR_MESSAGE_BYTES)
        self.assertIn("…[truncated]", out)

    def test_redacts_secret_paths(self):
        msg = (
            "open(/run/secrets/slskd_api_key) failed; "
            "fallback /var/lib/cratedigger/config.ini also missing; "
            "user home /home/abl030/.config/beets/secrets.yaml"
        )
        out = sanitize_error_message(msg)
        assert out is not None
        self.assertNotIn("/run/secrets/", out)
        self.assertNotIn("/var/lib/cratedigger/", out)
        self.assertNotIn("/home/abl030/", out)
        self.assertIn("[REDACTED-PATH]", out)

    def test_redacts_secret_shaped_tokens(self):
        # 40-char base64-shaped token (would match a real API key).
        token = "abcdEFGH1234abcdEFGH1234abcdEFGH1234abcd"
        msg = f"connection refused token={token}"
        out = sanitize_error_message(msg)
        assert out is not None
        self.assertNotIn(token, out)
        self.assertIn("[REDACTED-SECRET]", out)

    def test_passes_none_through(self):
        self.assertIsNone(sanitize_error_message(None))

    def test_provenance_dict_walked_recursively(self):
        prov = {
            "snapshot_signature": {"path": "/run/secrets/slskd_api_key"},
            "messages": ["short", "/etc/passwd was missing"],
        }
        out = sanitize_provenance(prov)
        assert out is not None
        self.assertIn("[REDACTED-PATH]",
                      out["snapshot_signature"]["path"])
        self.assertIn("[REDACTED-PATH]", out["messages"][1])

    def test_self_referential_dict_does_not_recurse_forever(self):
        """A self-referential provenance dict must not blow the stack;
        the cycle is replaced with a sentinel string."""
        prov: dict[str, object] = {"name": "loop"}
        prov["self"] = prov
        # Must not raise RecursionError.
        out = sanitize_provenance(prov)
        assert out is not None
        # The original level was sanitized; the cycle marker appears
        # somewhere inside the structure.
        flat = repr(out)
        self.assertIn("[CYCLE]", flat)

    def test_deeply_nested_dict_is_truncated(self):
        """Excessive nesting depth must be truncated rather than recursing
        without bound."""
        # Build a 30-deep dict; sanitizer should truncate beyond its cap.
        deep: dict[str, object] = {}
        cur: dict[str, object] = deep
        for _ in range(30):
            inner: dict[str, object] = {}
            cur["next"] = inner
            cur = inner
        cur["leaf"] = "value"
        out = sanitize_provenance(deep)
        assert out is not None
        flat = repr(out)
        self.assertIn("[TRUNCATED]", flat)


class TestSearchPlanServiceHistoryPage(unittest.TestCase):
    """U1 service contract: ``SearchPlanService.history_for_request``.

    Thin wrapper around ``PipelineDB.get_search_history_page`` that adds:
      * 404 mapping when the request_id does not exist
      * input-validation mapping when ``limit`` ∉ [1, 200] or
        ``before_id`` < 1.
      * Forwards rows + ``next_before_id`` straight from the DB result.

    Mirrors ``advance_for_request`` shape: returns a typed
    ``HistoryPageResult`` with an ``outcome`` string the route + CLI can
    branch on with no logic.
    """

    def setUp(self):
        self.db = FakePipelineDB()
        self.cfg = CratediggerConfig()
        self.svc = SearchPlanService(self.db, self.cfg)

    def _seed(self, rid: int, n: int) -> None:
        _seed_request(self.db, id=rid, artist_name="A", album_title="B",
                      year=2020)
        for i in range(n):
            self.db.log_search(rid, query=f"q{i}", outcome="no_match")

    def test_success_returns_rows_and_next_before_id(self):
        from lib.search_plan_service import (
            RESULT_HISTORY_PAGE_SUCCESS,
        )
        self._seed(rid=1, n=5)
        result = self.svc.history_for_request(1, limit=3)
        self.assertEqual(result.outcome, RESULT_HISTORY_PAGE_SUCCESS)
        self.assertEqual(result.request_id, 1)
        self.assertEqual(len(result.rows), 3)
        # Newest first; cursor seeds next page.
        self.assertEqual(result.rows[0]["query"], "q4")
        self.assertIsNotNone(result.next_before_id)

    def test_success_exhausted_when_fewer_rows_than_limit(self):
        from lib.search_plan_service import (
            RESULT_HISTORY_PAGE_SUCCESS,
        )
        self._seed(rid=1, n=2)
        result = self.svc.history_for_request(1, limit=10)
        self.assertEqual(result.outcome, RESULT_HISTORY_PAGE_SUCCESS)
        self.assertEqual(len(result.rows), 2)
        self.assertIsNone(result.next_before_id)

    def test_success_resumes_from_cursor(self):
        self._seed(rid=1, n=5)
        first = self.svc.history_for_request(1, limit=3)
        second = self.svc.history_for_request(
            1, limit=3, before_id=first.next_before_id,
        )
        self.assertEqual(len(second.rows), 2)
        self.assertEqual(second.rows[0]["query"], "q1")
        self.assertEqual(second.rows[1]["query"], "q0")
        self.assertIsNone(second.next_before_id)

    def test_request_not_found_returns_request_not_found(self):
        from lib.search_plan_service import (
            RESULT_REQUEST_NOT_FOUND,
        )
        result = self.svc.history_for_request(9999, limit=10)
        self.assertEqual(result.outcome, RESULT_REQUEST_NOT_FOUND)
        self.assertEqual(result.rows, [])
        self.assertIsNone(result.next_before_id)
        self.assertIsNotNone(result.error_message)

    def test_limit_zero_returns_input_validation(self):
        from lib.search_plan_service import (
            RESULT_HISTORY_PAGE_INPUT_INVALID,
        )
        self._seed(rid=1, n=2)
        result = self.svc.history_for_request(1, limit=0)
        self.assertEqual(result.outcome, RESULT_HISTORY_PAGE_INPUT_INVALID)
        self.assertEqual(result.rows, [])
        self.assertIn("[1, 200]", result.error_message or "")

    def test_limit_above_max_returns_input_validation(self):
        from lib.search_plan_service import (
            RESULT_HISTORY_PAGE_INPUT_INVALID,
        )
        self._seed(rid=1, n=2)
        result = self.svc.history_for_request(1, limit=201)
        self.assertEqual(result.outcome, RESULT_HISTORY_PAGE_INPUT_INVALID)
        self.assertIn("[1, 200]", result.error_message or "")

    def test_limit_negative_returns_input_validation(self):
        from lib.search_plan_service import (
            RESULT_HISTORY_PAGE_INPUT_INVALID,
        )
        self._seed(rid=1, n=2)
        result = self.svc.history_for_request(1, limit=-1)
        self.assertEqual(result.outcome, RESULT_HISTORY_PAGE_INPUT_INVALID)

    def test_before_id_zero_returns_input_validation(self):
        from lib.search_plan_service import (
            RESULT_HISTORY_PAGE_INPUT_INVALID,
        )
        self._seed(rid=1, n=2)
        result = self.svc.history_for_request(1, limit=10, before_id=0)
        self.assertEqual(result.outcome, RESULT_HISTORY_PAGE_INPUT_INVALID)
        self.assertIn("before_id", result.error_message or "")

    def test_before_id_negative_returns_input_validation(self):
        from lib.search_plan_service import (
            RESULT_HISTORY_PAGE_INPUT_INVALID,
        )
        self._seed(rid=1, n=2)
        result = self.svc.history_for_request(1, limit=10, before_id=-1)
        self.assertEqual(result.outcome, RESULT_HISTORY_PAGE_INPUT_INVALID)


class TestSearchPlanConfigFromCratedigger(unittest.TestCase):
    def test_threshold_propagates(self):
        cfg = CratediggerConfig(search_escalation_threshold=7)
        plan_cfg = search_plan_config_from_cratedigger_config(cfg)
        self.assertEqual(plan_cfg.escalation_threshold, 7)

    def test_default_threshold_matches_search_module_default(self):
        cfg = CratediggerConfig()
        plan_cfg = search_plan_config_from_cratedigger_config(cfg)
        self.assertEqual(plan_cfg.escalation_threshold, 5)


class TestAdvisoryLockBoundary(unittest.TestCase):
    """The service must take the per-request PLAN advisory lock around
    every persistence path so concurrent CLI + web + startup callers
    cannot trip the partial-unique active-plan index."""

    def test_lock_acquired_for_successful_persist(self):
        db = FakePipelineDB()
        _seed_request(db, id=30, artist_name="Khruangbin",
                       album_title="Mordechai", year=2020)
        db.set_tracks(30, _ok_tracks())

        cfg = CratediggerConfig()
        svc = SearchPlanService(db, cfg)
        result = svc.generate_for_new_request(
            30,
            artist_name="Khruangbin", album_title="Mordechai",
            year=2020, tracks=_ok_tracks(),
        )
        self.assertEqual(result.outcome, RESULT_SUCCESS)
        from lib.pipeline_db import ADVISORY_LOCK_NAMESPACE_PLAN
        self.assertIn(
            (ADVISORY_LOCK_NAMESPACE_PLAN, 30),
            db.advisory_lock_calls,
        )

    def test_contention_yields_transient_failure(self):
        db = FakePipelineDB()
        _seed_request(db, id=31, artist_name="Stereolab",
                       album_title="Mars Audiac Quintet", year=1994)
        db.set_tracks(31, _ok_tracks())
        # Force the advisory lock to be unavailable.
        db.set_advisory_lock_result(False)
        svc = SearchPlanService(db, CratediggerConfig())
        result = svc.generate_for_new_request(
            31,
            artist_name="Stereolab", album_title="Mars Audiac Quintet",
            year=1994, tracks=_ok_tracks(),
        )
        self.assertEqual(result.outcome, RESULT_FAILED_TRANSIENT)
        self.assertEqual(result.failure_class,
                         FAILURE_CLASS_DEPENDENCY_FAILURE)
        self.assertEqual(db.search_plans, {})


class TestSearchPlanServiceAdvance(unittest.TestCase):
    """Operator-driven cursor advance — counterpart of regenerate, used to
    skip past collapsed default-strategy slots on self-titled releases.

    The CLI (``pipeline-cli search-plan advance``) and web API (``POST
    /api/pipeline/<id>/search-plan/advance``) are thin wrappers over
    ``SearchPlanService.advance_for_request``; coverage here protects both
    surfaces. See ``CLAUDE.md`` § "CLI ⇄ API surface symmetry"."""

    def setUp(self):
        self.db = FakePipelineDB()
        self.cfg = CratediggerConfig()
        self.svc = SearchPlanService(self.db, self.cfg)

    def _seed_plan_with_items(self) -> int:
        """Seed a Bowie-style plan: 5 default + 1 unwild + 1 unwild_year +
        2 track_X items. Returns the request id (10)."""
        _seed_request(self.db, id=10,
                       artist_name="David Bowie",
                       album_title="David Bowie", year=1967)
        from lib.pipeline_db import SearchPlanItemInput
        items = [
            SearchPlanItemInput(ordinal=i, strategy="default",
                                query="*avid *owie",
                                canonical_query_key="*avid *owie")
            for i in range(5)
        ]
        items.append(SearchPlanItemInput(
            ordinal=5, strategy="unwild", query="David Bowie",
            canonical_query_key="david bowie"))
        items.append(SearchPlanItemInput(
            ordinal=6, strategy="unwild_year", query="David Bowie 1967",
            canonical_query_key="david bowie 1967"))
        items.append(SearchPlanItemInput(
            ordinal=7, strategy="track_0", query="Love Till Tuesday",
            canonical_query_key="love till tuesday"))
        items.append(SearchPlanItemInput(
            ordinal=8, strategy="track_1", query="Maids Bond Street",
            canonical_query_key="maids bond street"))
        plan = self.db.create_successful_search_plan(
            request_id=10, generator_id=SEARCH_PLAN_GENERATOR_ID,
            items=items)
        return plan

    def test_advance_to_ordinal_moves_cursor_forward(self):
        """Happy path: explicit ordinal advance updates cursor and reports
        the new slot."""
        from lib.search_plan_service import RESULT_ADVANCED
        self._seed_plan_with_items()
        result = self.svc.advance_for_request(10, to_ordinal=7)
        self.assertEqual(result.outcome, RESULT_ADVANCED)
        self.assertEqual(result.previous_ordinal, 0)
        self.assertEqual(result.new_ordinal, 7)
        self.assertEqual(result.new_strategy, "track_0")
        self.assertEqual(result.new_query, "Love Till Tuesday")
        active = self.db.get_active_search_plan(10)
        assert active is not None
        self.assertEqual(active.next_ordinal, 7)

    def test_advance_to_strategy_finds_first_matching_slot(self):
        """``--to-strategy track`` jumps to the first ``track_*`` slot past
        the cursor — the motivating use case for self-titled releases."""
        from lib.search_plan_service import RESULT_ADVANCED
        self._seed_plan_with_items()
        result = self.svc.advance_for_request(10, to_strategy="track")
        self.assertEqual(result.outcome, RESULT_ADVANCED)
        self.assertEqual(result.new_ordinal, 7)
        self.assertEqual(result.new_strategy, "track_0")

    def test_advance_to_strategy_unwild_year_exact(self):
        """Strategy prefix matches exact strategy names too."""
        from lib.search_plan_service import RESULT_ADVANCED
        self._seed_plan_with_items()
        result = self.svc.advance_for_request(10, to_strategy="unwild_year")
        self.assertEqual(result.outcome, RESULT_ADVANCED)
        self.assertEqual(result.new_ordinal, 6)
        self.assertEqual(result.new_strategy, "unwild_year")

    def test_advance_backward_is_rejected(self):
        """Forward-only: target <= current cursor returns INVALID_TARGET."""
        from lib.search_plan_service import RESULT_INVALID_TARGET
        self._seed_plan_with_items()
        # First advance to 5 so cursor isn't at 0
        self.svc.advance_for_request(10, to_ordinal=5)
        result = self.svc.advance_for_request(10, to_ordinal=3)
        self.assertEqual(result.outcome, RESULT_INVALID_TARGET)
        self.assertIsNotNone(result.error_message)
        # Cursor unchanged.
        active = self.db.get_active_search_plan(10)
        assert active is not None
        self.assertEqual(active.next_ordinal, 5)

    def test_advance_to_same_ordinal_is_rejected(self):
        """target == current is also forward-only-violating."""
        from lib.search_plan_service import RESULT_INVALID_TARGET
        self._seed_plan_with_items()
        result = self.svc.advance_for_request(10, to_ordinal=0)
        self.assertEqual(result.outcome, RESULT_INVALID_TARGET)

    def test_advance_to_out_of_range_ordinal(self):
        from lib.search_plan_service import RESULT_INVALID_TARGET
        self._seed_plan_with_items()  # 9 items, indices 0..8
        result = self.svc.advance_for_request(10, to_ordinal=99)
        self.assertEqual(result.outcome, RESULT_INVALID_TARGET)

    def test_advance_to_strategy_no_match(self):
        from lib.search_plan_service import RESULT_INVALID_TARGET
        self._seed_plan_with_items()
        result = self.svc.advance_for_request(10, to_strategy="nonexistent")
        self.assertEqual(result.outcome, RESULT_INVALID_TARGET)

    def test_advance_no_active_plan(self):
        """Request exists but has no active plan → NO_ACTIVE_PLAN."""
        from lib.search_plan_service import RESULT_NO_ACTIVE_PLAN
        _seed_request(self.db, id=20, artist_name="X", album_title="Y")
        result = self.svc.advance_for_request(20, to_ordinal=1)
        self.assertEqual(result.outcome, RESULT_NO_ACTIVE_PLAN)

    def test_advance_request_not_found(self):
        from lib.search_plan_service import RESULT_REQUEST_NOT_FOUND
        result = self.svc.advance_for_request(9999, to_ordinal=1)
        self.assertEqual(result.outcome, RESULT_REQUEST_NOT_FOUND)

    def test_advance_neither_target_provided(self):
        from lib.search_plan_service import RESULT_INVALID_TARGET
        self._seed_plan_with_items()
        result = self.svc.advance_for_request(10)
        self.assertEqual(result.outcome, RESULT_INVALID_TARGET)

    def test_advance_both_targets_provided(self):
        from lib.search_plan_service import RESULT_INVALID_TARGET
        self._seed_plan_with_items()
        result = self.svc.advance_for_request(
            10, to_ordinal=7, to_strategy="track")
        self.assertEqual(result.outcome, RESULT_INVALID_TARGET)


if __name__ == "__main__":
    unittest.main()
