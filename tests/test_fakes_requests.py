"""Self-tests for the FakePipelineDB album_requests cluster.

Split out of ``tests/test_fakes.py`` (#1313) so each cluster of the
fake has its sibling tests beside it.
"""
import copy
import unittest
from datetime import UTC, datetime, timedelta

from lib.pipeline_db import RequestSpectralStateUpdate
from lib.pipeline_db._shared import REQUEST_METADATA_RESERVED_FIELDS
from lib.quality import SpectralMeasurement
from tests.evidence_helpers import make_album_quality_evidence
from tests.fakes import (
    FakePipelineDB,
)
from tests.helpers import (
    make_request_row,
)


class TestFakePipelineDBDiscogs(unittest.TestCase):
    """Tests for Discogs-related FakePipelineDB methods."""

    def test_get_request_by_mb_release_id_found(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, mb_release_id="abc-uuid"))
        result = db.get_request_by_mb_release_id("abc-uuid")
        assert result is not None
        self.assertEqual(result["id"], 1)

    def test_get_request_by_mb_release_id_not_found(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, mb_release_id="abc-uuid"))
        self.assertIsNone(db.get_request_by_mb_release_id("other"))

    def test_get_request_by_discogs_release_id_found(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, discogs_release_id="12345"))
        result = db.get_request_by_discogs_release_id("12345")
        assert result is not None
        self.assertEqual(result["id"], 1)

    def test_get_request_by_discogs_release_id_not_found(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, discogs_release_id="12345"))
        self.assertIsNone(db.get_request_by_discogs_release_id("99999"))

    def test_get_request_by_release_id_normalizes_uppercase_uuid(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=1,
            mb_release_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        ))
        result = db.get_request_by_release_id("AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA")
        assert result is not None
        self.assertEqual(result["id"], 1)

    def test_get_request_by_release_id_falls_back_to_legacy_numeric_mb_column(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=1,
            mb_release_id="12856590",
            discogs_release_id=None,
        ))
        result = db.get_request_by_release_id("0012856590")
        assert result is not None
        self.assertEqual(result["id"], 1)


class TestFakeSupersedeRequestMbid(unittest.TestCase):
    """U3: ``FakePipelineDB.supersede_request_mbid`` + companions for
    the Replace operator action.
    """

    def _seed_old(self, **overrides):
        db = FakePipelineDB()
        row = make_request_row(
            id=42,
            mb_release_id="old-mbid",
            mb_release_group_id="rg-1",
            mb_artist_id="art-1",
            artist_name="Pet Grief",
            album_title="Old Album",
            year=2024,
            country="US",
            status="imported",
            verified_lossless=True,
            current_spectral_grade="A",
            current_spectral_bitrate=900,
            current_lossless_source_v0_probe_min_bitrate=235,
            current_lossless_source_v0_probe_avg_bitrate=245,
            current_lossless_source_v0_probe_median_bitrate=240,
            search_filetype_override="lossless",
            target_format="flac",
            min_bitrate=900,
            source="request",
        )
        for k, v in overrides.items():
            row[k] = v
        db.seed_request(row)
        return db

    def test_happy_path_flips_old_inserts_new(self):
        db = self._seed_old()
        new_id = db.supersede_request_mbid(
            42,
            new_mb_release_id="new-mbid",
            new_mb_release_group_id="rg-1",
            new_mb_artist_id="art-1",
            new_artist_name="Pet Grief",
            new_album_title="New Album",
            new_year=2025,
            new_country="JP",
            new_tracks=[
                {"disc_number": 1, "track_number": 1, "title": "T1"},
                {"disc_number": 1, "track_number": 2, "title": "T2"},
            ],
        )
        old = db.get_request(42)
        assert old is not None
        self.assertEqual(old["status"], "replaced")
        new = db.get_request(new_id)
        assert new is not None
        self.assertEqual(new["mb_release_id"], "new-mbid")
        self.assertEqual(new["status"], "wanted")
        self.assertEqual(new["replaces_request_id"], 42)
        self.assertEqual(new["source"], "request")  # inherited
        self.assertEqual(len(db.get_tracks(new_id)), 2)

    def test_discogs_release_id_threaded_onto_new_row(self):
        # U1: a Discogs-pathway supersede dual-writes discogs_release_id onto
        # the new row — the fake must thread it identically to real PG.
        db = self._seed_old()
        new_id = db.supersede_request_mbid(
            42,
            new_mb_release_id="new-mbid",
            new_mb_release_group_id="rg-1",
            new_mb_artist_id="art-1",
            new_artist_name="Pet Grief",
            new_album_title="New Album",
            new_year=2025,
            new_country="JP",
            new_discogs_release_id="12345",
            new_tracks=[],
        )
        new = db.get_request(new_id)
        assert new is not None
        self.assertEqual(new["discogs_release_id"], "12345")

    def test_discogs_release_id_defaults_to_none(self):
        # MB Replace omits new_discogs_release_id — the new row's column is None.
        db = self._seed_old()
        new_id = db.supersede_request_mbid(
            42,
            new_mb_release_id="new-mbid",
            new_mb_release_group_id="rg-1",
            new_mb_artist_id="art-1",
            new_artist_name="Pet Grief",
            new_album_title="New Album",
            new_year=2025,
            new_country="JP",
            new_tracks=[],
        )
        new = db.get_request(new_id)
        assert new is not None
        self.assertIsNone(new["discogs_release_id"])

    def test_characteristic_fields_preserved_on_old_row(self):
        db = self._seed_old()
        db.supersede_request_mbid(
            42,
            new_mb_release_id="new-mbid",
            new_mb_release_group_id="rg-1",
            new_mb_artist_id="art-1",
            new_artist_name="Pet Grief",
            new_album_title="New Album",
            new_year=2025,
            new_country="JP",
            new_tracks=[],
        )
        old = db.get_request(42)
        assert old is not None
        # Characteristic fields stay frozen on the audit row.
        self.assertEqual(old["mb_release_id"], "old-mbid")
        self.assertEqual(old["mb_release_group_id"], "rg-1")
        self.assertEqual(old["mb_artist_id"], "art-1")
        self.assertEqual(old["artist_name"], "Pet Grief")
        self.assertEqual(old["album_title"], "Old Album")
        self.assertEqual(old["year"], 2024)
        self.assertEqual(old["country"], "US")
        self.assertEqual(old["min_bitrate"], 900)
        self.assertTrue(old["verified_lossless"])
        self.assertEqual(old["current_spectral_grade"], "A")
        self.assertEqual(old["current_spectral_bitrate"], 900)
        self.assertEqual(old["current_lossless_source_v0_probe_min_bitrate"], 235)
        self.assertEqual(old["current_lossless_source_v0_probe_avg_bitrate"], 245)
        self.assertEqual(old["current_lossless_source_v0_probe_median_bitrate"], 240)
        self.assertEqual(old["search_filetype_override"], "lossless")
        self.assertEqual(old["target_format"], "flac")

    def test_collision_raises(self):
        from lib.pipeline_db import MbidCollisionError

        db = self._seed_old()
        db.seed_request(make_request_row(
            id=99, mb_release_id="collide-mbid", mb_release_group_id="rg-2",
        ))
        with self.assertRaises(MbidCollisionError):
            db.supersede_request_mbid(
                42,
                new_mb_release_id="collide-mbid",
                new_mb_release_group_id="rg-1",
                new_mb_artist_id=None,
                new_artist_name="x", new_album_title="x",
                new_year=None, new_country=None, new_tracks=[],
            )

    def test_race_on_already_replaced_raises(self):
        from lib.pipeline_db import SupersedeRaceError

        db = self._seed_old(status="replaced")
        with self.assertRaises(SupersedeRaceError):
            db.supersede_request_mbid(
                42,
                new_mb_release_id="new-mbid",
                new_mb_release_group_id="rg-1",
                new_mb_artist_id=None,
                new_artist_name="x", new_album_title="x",
                new_year=None, new_country=None, new_tracks=[],
            )

    def test_list_requests_in_rg_excludes_replaced_by_default(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=1, mb_release_id="a", mb_release_group_id="rg-x", status="wanted",
        ))
        db.seed_request(make_request_row(
            id=2, mb_release_id="b", mb_release_group_id="rg-x", status="replaced",
        ))
        rows = db.list_requests_in_release_group("rg-x")
        self.assertEqual([r["id"] for r in rows], [1])

    def test_list_requests_in_rg_include_replaced(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=1, mb_release_id="a", mb_release_group_id="rg-x", status="wanted",
        ))
        db.seed_request(make_request_row(
            id=2, mb_release_id="b", mb_release_group_id="rg-x", status="replaced",
        ))
        rows = db.list_requests_in_release_group("rg-x", exclude_replaced=False)
        # Newest first (id desc).
        self.assertEqual([r["id"] for r in rows], [2, 1])

    def test_list_requests_in_rg_exclude_request_id(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=1, mb_release_id="a", mb_release_group_id="rg-x", status="wanted",
        ))
        db.seed_request(make_request_row(
            id=2, mb_release_id="b", mb_release_group_id="rg-x", status="wanted",
        ))
        rows = db.list_requests_in_release_group("rg-x", exclude_request_id=1)
        self.assertEqual([r["id"] for r in rows], [2])

    def test_list_active_release_group_ids(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=1, mb_release_id="a", mb_release_group_id="rg-1", status="wanted",
        ))
        db.seed_request(make_request_row(
            id=2, mb_release_id="b", mb_release_group_id="rg-2", status="downloading",
        ))
        db.seed_request(make_request_row(
            id=3, mb_release_id="c", mb_release_group_id="rg-3", status="replaced",
        ))
        db.seed_request(make_request_row(
            id=4, mb_release_id="d", mb_release_group_id=None, status="wanted",
        ))
        self.assertEqual(
            db.list_active_release_group_ids(), {"rg-1", "rg-2"}
        )

    def test_list_active_release_group_ids_empty(self):
        db = FakePipelineDB()
        self.assertEqual(db.list_active_release_group_ids(), set())

    def test_list_non_replaced_requests_excludes_replaced_and_sorts_by_id(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=2, status="wanted"))
        db.seed_request(make_request_row(id=1, status="imported"))
        db.seed_request(make_request_row(id=3, status="replaced"))

        rows = db.list_non_replaced_requests()

        self.assertEqual([r["id"] for r in rows], [1, 2])

    def test_get_request_by_replaces_request_id_found(self):
        db = self._seed_old()
        new_id = db.supersede_request_mbid(
            42,
            new_mb_release_id="new-mbid",
            new_mb_release_group_id="rg-1",
            new_mb_artist_id=None,
            new_artist_name="x", new_album_title="x",
            new_year=None, new_country=None, new_tracks=[],
        )
        descendant = db.get_request_by_replaces_request_id(42)
        assert descendant is not None
        self.assertEqual(descendant["id"], new_id)

    def test_get_request_by_replaces_request_id_none(self):
        db = self._seed_old()
        self.assertIsNone(db.get_request_by_replaces_request_id(42))

    def test_get_oldest_request_chain_created_at_walks_the_chain(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=10, status="replaced",
            created_at=datetime(2026, 2, 1, tzinfo=UTC)))
        db.seed_request(make_request_row(
            id=11, status="replaced", replaces_request_id=10,
            created_at=datetime(2026, 4, 1, tzinfo=UTC)))
        db.seed_request(make_request_row(
            id=12, replaces_request_id=11,
            created_at=datetime(2026, 6, 1, tzinfo=UTC)))
        self.assertEqual(
            db.get_oldest_request_chain_created_at(12),
            datetime(2026, 2, 1, tzinfo=UTC))
        # A chain head returns its own created_at.
        self.assertEqual(
            db.get_oldest_request_chain_created_at(10),
            datetime(2026, 2, 1, tzinfo=UTC))

    def test_get_oldest_request_chain_created_at_unknown_id_is_none(self):
        db = FakePipelineDB()
        self.assertIsNone(db.get_oldest_request_chain_created_at(999))

    def test_denylist_isolation_old_keeps_new_empty(self):
        """A supersede must not copy denylist entries from the old
        request onto the new row — the new request starts fresh
        (R28). The old row's denylist is preserved unchanged as part
        of the audit trail."""
        db = self._seed_old()
        # Seed two denylist entries on the old row.
        db.add_denylist(42, "bad_peer_1", reason="lossy_source")
        db.add_denylist(42, "bad_peer_2", reason="incomplete")
        new_id = db.supersede_request_mbid(
            42,
            new_mb_release_id="new-mbid",
            new_mb_release_group_id="rg-1",
            new_mb_artist_id=None,
            new_artist_name="x", new_album_title="x",
            new_year=None, new_country=None, new_tracks=[],
        )
        # Old row's denylist is intact.
        old_denylist = db.get_denylisted_users(42)
        self.assertEqual(
            sorted(d["username"] for d in old_denylist),
            ["bad_peer_1", "bad_peer_2"],
        )
        # New row's denylist is empty — denylist is per-request and
        # supersede does NOT propagate.
        new_denylist = db.get_denylisted_users(new_id)
        self.assertEqual(new_denylist, [])


class TestFakePipelineDBUnfindable(unittest.TestCase):
    """Self-tests for U13 ``FakePipelineDB`` unfindable-detection writers.

    Follows the New Work Checklist row in
    ``.claude/rules/code-quality.md``, which asks a new ``PipelineDB``
    method for an equivalent stub on ``FakePipelineDB`` plus a self-test
    in that cluster's test module. Each test exercises a single fake
    method's contract: call recording and persisted row state.
    """

    def test_record_artist_probe_writes_and_records(self) -> None:
        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="A", album_title="B", source="request",
            mb_release_id="m-uf-1",
        )
        ts = datetime(2026, 5, 26, tzinfo=UTC)
        db.record_artist_probe(rid, match_count=7, observed_at=ts)
        # Call recorder.
        self.assertEqual(
            db.record_artist_probe_calls,
            [(rid, 7, ts)],
        )
        # Row state.
        row = db.request(rid)
        self.assertEqual(row["last_artist_probe_at"], ts)
        self.assertEqual(row["last_artist_probe_match_count"], 7)
        self.assertEqual(row["updated_at"], ts)

    def test_set_unfindable_category_validates_vocabulary(self) -> None:
        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="A", album_title="B", source="request",
            mb_release_id="m-uf-2",
        )
        ts = datetime(2026, 5, 26, tzinfo=UTC)
        # Valid: write a category.
        db.set_unfindable_category(
            rid, category="artist_absent", categorised_at=ts,
        )
        row = db.request(rid)
        self.assertEqual(row["unfindable_category"], "artist_absent")
        self.assertEqual(row["unfindable_categorised_at"], ts)
        # Valid: clear (None).
        ts2 = ts + timedelta(days=1)
        db.set_unfindable_category(rid, category=None, categorised_at=ts2)
        row = db.request(rid)
        self.assertIsNone(row["unfindable_category"])
        self.assertEqual(row["unfindable_categorised_at"], ts2)
        # Invalid vocabulary: raises (mirrors production CHECK).
        with self.assertRaises(ValueError):
            db.set_unfindable_category(
                rid, category="garbage", categorised_at=ts,
            )

    def test_list_unfindable_probe_candidates_orders_oldest_first(self) -> None:
        db = FakePipelineDB()
        now = datetime.now(UTC)
        # NULL probe → sorts first.
        rid_null = db.add_request(
            artist_name="Null", album_title="X", source="request",
            mb_release_id="m-cand-null",
        )
        # 10d old probe → eligible (window=7).
        rid_old = db.add_request(
            artist_name="Old", album_title="X", source="request",
            mb_release_id="m-cand-old",
        )
        db.update_request_fields(
            rid_old, last_artist_probe_at=now - timedelta(days=10),
            last_artist_probe_match_count=0,
        )
        # 1d old → ineligible.
        rid_fresh = db.add_request(
            artist_name="Fresh", album_title="X", source="request",
            mb_release_id="m-cand-fresh",
        )
        db.update_request_fields(
            rid_fresh, last_artist_probe_at=now - timedelta(days=1),
        )
        # Not wanted → ineligible.
        rid_imp = db.add_request(
            artist_name="Imp", album_title="X", source="request",
            mb_release_id="m-cand-imp", status="imported",
        )

        cands = db.list_unfindable_probe_candidates(
            limit=10, probe_interval_days=7,
        )
        cand_ids = [c["id"] for c in cands]
        self.assertEqual(cand_ids[0], rid_null)
        self.assertIn(rid_old, cand_ids)
        self.assertNotIn(rid_fresh, cand_ids)
        self.assertNotIn(rid_imp, cand_ids)

    def test_list_unfindable_probe_candidates_respects_limit(self) -> None:
        db = FakePipelineDB()
        for i in range(5):
            db.add_request(
                artist_name=f"A{i}", album_title="X", source="request",
                mb_release_id=f"m-lim-{i}",
            )
        cands = db.list_unfindable_probe_candidates(
            limit=2, probe_interval_days=7,
        )
        self.assertEqual(len(cands), 2)

    def test_get_unfindable_search_log_signal_aggregates_correctly(self) -> None:
        from lib.unfindable_detection_service import (
            UnfindableSearchLogSignal,
        )

        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="A", album_title="B", source="request",
            mb_release_id="m-sig",
        )
        # Cycle 0: one no_match (zero find), one wrong-pressing hit.
        db.log_search(
            request_id=rid, outcome="no_match", query="q1",
            rejection_reason="strict_count_mismatch",
            matcher_score_top1=0.9,
        )
        db.search_logs[-1].attempt_consumed = True
        db.search_logs[-1].plan_cycle_snapshot = 0
        # Cycle 1: one found (NOT zero find).
        db.log_search(request_id=rid, outcome="found", query="q2")
        db.search_logs[-1].attempt_consumed = True
        db.search_logs[-1].plan_cycle_snapshot = 1
        # Cycle 2: one no_match, score below threshold → not a hit.
        db.log_search(
            request_id=rid, outcome="no_match", query="q3",
            rejection_reason="strict_count_mismatch",
            matcher_score_top1=0.5,
        )
        db.search_logs[-1].attempt_consumed = True
        db.search_logs[-1].plan_cycle_snapshot = 2
        # Cycle 3: non-consumed (stale completion) — filtered out.
        db.log_search(request_id=rid, outcome="no_match", query="stale")
        db.search_logs[-1].attempt_consumed = False
        db.search_logs[-1].plan_cycle_snapshot = 3

        sig = db.get_unfindable_search_log_signal(
            rid, window_days=30, matcher_score_threshold=0.85,
        )
        self.assertIsInstance(sig, UnfindableSearchLogSignal)
        self.assertEqual(sig.zero_find_cycles, 2)  # cycles 0 and 2
        self.assertEqual(sig.wrong_pressing_hits, 1)  # cycle 0 only

    def test_cursor_mutation_recorders_fire_on_real_mutators(self) -> None:
        """Sanity: the R20 runtime guard requires these to be observable.

        If the recorders ever stop firing on the real cursor-mutator
        methods, the R20 runtime test silently goes green even when
        the detection module starts touching them — defeating the
        point of the guard.
        """
        from lib.pipeline_db import (
            ConsumedAttemptInput,
            SearchPlanItemInput,
        )

        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="A", album_title="B", source="request",
            mb_release_id="m-cur-1",
        )
        plan_id = db.create_successful_search_plan(
            request_id=rid, generator_id="g1",
            items=[
                SearchPlanItemInput(
                    ordinal=0, strategy="s0", query="Q0",
                    canonical_query_key="q0",
                ),
                SearchPlanItemInput(
                    ordinal=1, strategy="s1", query="Q1",
                    canonical_query_key="q1",
                ),
            ],
        )
        active = db.get_active_search_plan(rid)
        assert active is not None
        attempt = ConsumedAttemptInput(
            request_id=rid, plan_id=plan_id,
            plan_item_id=active.items[0].id,
            plan_ordinal=0, plan_strategy="s0",
            plan_canonical_query_key="q0",
            plan_repeat_group=None, plan_generator_id="g1",
            query="Q0", outcome="no_results",
            plan_item_count=2, cycle_count_snapshot=0,
        )
        db.record_consumed_search_attempt(attempt)
        self.assertEqual(len(db.record_consumed_search_attempt_calls), 1)
        # advance_search_plan_cursor recorder. Use a separate request
        # with a fresh plan since the consumed-attempt above already
        # advanced this row's cursor to 1.
        rid2 = db.add_request(
            artist_name="A2", album_title="B2", source="request",
            mb_release_id="m-cur-2",
        )
        db.create_successful_search_plan(
            request_id=rid2, generator_id="g1",
            items=[
                SearchPlanItemInput(
                    ordinal=0, strategy="s0", query="Q0",
                    canonical_query_key="q0",
                ),
                SearchPlanItemInput(
                    ordinal=1, strategy="s1", query="Q1",
                    canonical_query_key="q1",
                ),
            ],
        )
        db.advance_search_plan_cursor(
            rid2, target_ordinal=1, plan_item_count=2,
        )
        self.assertGreaterEqual(len(db.advance_search_plan_cursor_calls), 1)


class TestFakePipelineDBRescueCapture(unittest.TestCase):
    """U14: ``FakePipelineDB.mark_imported_with_rescue`` self-tests.

    Mirrors the real-PG contract in ``test_pipeline_db.py``:
    happy-path rescue stamp, no-prior-category no-op, one-shot
    immutability after a prior rescue, and atomic semantics on the
    in-memory store (rollback simulation via patched commit).
    """

    UNFINDABLE_CATEGORIES = (
        "artist_absent",
        "album_absent_artist_present",
        "one_track_structural",
        "wrong_pressing_available",
    )

    def _seed_downloading(self, db, *, category=None, rescued_at=None,
                          prior_category=None):
        rid = db.add_request(
            artist_name="Rescue", album_title="Album",
            source="request",
            mb_release_id=f"m-rescue-{category or 'none'}",
        )
        # Set the unfindable category while still wanted —
        # ``set_unfindable_category`` is guarded by ``status='wanted'``
        # in production (lost-update protection against concurrent
        # rescue); the fake mirrors that guard so writes against
        # already-downloading rows would silently no-op.
        if category is not None:
            ts = datetime(2026, 5, 20, tzinfo=UTC)
            db.set_unfindable_category(
                rid, category=category, categorised_at=ts,
            )
        db.update_status(rid, "downloading", state_json="{}")
        if rescued_at is not None or prior_category is not None:
            db._requests[rid]["rescued_at"] = rescued_at
            db._requests[rid]["prior_unfindable_category"] = prior_category
        return rid

    def test_rescue_writes_three_columns_for_each_category(self) -> None:
        for category in self.UNFINDABLE_CATEGORIES:
            with self.subTest(category=category):
                db = FakePipelineDB()
                rid = self._seed_downloading(db, category=category)

                db.mark_imported_with_rescue(rid, beets_distance=0.05)

                row = db.request(rid)
                self.assertEqual(row["status"], "imported")
                self.assertIsNone(row["unfindable_category"])
                self.assertEqual(
                    row["prior_unfindable_category"], category)
                self.assertIsNotNone(row["rescued_at"])
                # Imported-side extras still flow through.
                self.assertEqual(row["beets_distance"], 0.05)
                # status_history records the transition.
                self.assertIn((rid, "imported"), db.status_history)

    def test_no_rescue_stamp_when_unfindable_was_null(self) -> None:
        db = FakePipelineDB()
        rid = self._seed_downloading(db, category=None)

        db.mark_imported_with_rescue(rid, beets_distance=0.1)

        row = db.request(rid)
        self.assertEqual(row["status"], "imported")
        self.assertIsNone(row["rescued_at"])
        self.assertIsNone(row["prior_unfindable_category"])
        self.assertIsNone(row["unfindable_category"])

    def test_first_rescue_wins_re_import_is_a_noop_on_audit_columns(
        self,
    ) -> None:
        db = FakePipelineDB()
        original_rescue_at = datetime(2026, 1, 15, tzinfo=UTC)
        rid = self._seed_downloading(
            db,
            category="wrong_pressing_available",
            rescued_at=original_rescue_at,
            prior_category="artist_absent",
        )

        db.mark_imported_with_rescue(rid, beets_distance=0.05)

        row = db.request(rid)
        self.assertEqual(row["status"], "imported")
        self.assertEqual(row["rescued_at"], original_rescue_at)
        self.assertEqual(row["prior_unfindable_category"], "artist_absent")
        # The current category is still cleared.
        self.assertIsNone(row["unfindable_category"])


class TestFakeGetPipelineOverlay(unittest.TestCase):
    def test_projects_overlay_fields_from_seeded_requests(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=7, mb_release_id="mbid-1", status="wanted",
            search_filetype_override="lossless", min_bitrate=900))
        db.seed_request(make_request_row(id=8, mb_release_id="mbid-2"))
        info = db.get_pipeline_overlay(["mbid-1", "mbid-unknown"])
        self.assertEqual(set(info), {"mbid-1"})
        self.assertEqual(info["mbid-1"], {
            "id": 7, "status": "wanted",
            "search_filetype_override": "lossless",
            "target_format": None, "min_bitrate": 900,
            "has_captured_history": False,
            "verified_lossless": False,
            "provisional_lossless": False,
            "processing_owner": None,
        })

    def test_empty_mbids_short_circuits(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=7, mb_release_id="mbid-1"))
        self.assertEqual(db.get_pipeline_overlay([]), {})


class TestFakeListLibraryRequestCandidates(unittest.TestCase):
    def test_preserves_duplicate_and_legacy_discogs_cardinality(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=7,
            mb_release_id=None,
            discogs_release_id="12856590",
        ))
        db.seed_request(make_request_row(
            id=8,
            mb_release_id=None,
            discogs_release_id="12856590",
        ))
        db.seed_request(make_request_row(
            id=9,
            mb_release_id="12856590",
            discogs_release_id=None,
        ))
        db.seed_request(make_request_row(
            id=10,
            mb_release_id="not-a-release-id",
            discogs_release_id="12856590",
        ))

        rows = db.list_library_request_candidates(["12856590"])

        self.assertEqual([row["id"] for row in rows], [7, 8, 9])

    def test_empty_or_malformed_ids_have_no_candidates(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=7,
            mb_release_id="not-a-release-id",
        ))

        self.assertEqual(db.list_library_request_candidates([]), [])
        self.assertEqual(
            db.list_library_request_candidates(["not-a-release-id"]),
            [],
        )


class TestFakeRequestUniqueMbReleaseId(unittest.TestCase):
    """The fake mirrors migrations/001's UNIQUE on album_requests.mb_release_id.

    Test-fidelity Rule B — the fake must not be more permissive than the
    real INSERT. Two rows sharing a non-NULL mb_release_id is a state
    production can never hold (#445 item 4).
    """

    def test_seed_request_rejects_duplicate_mb_release_id(self):
        import psycopg2.errors

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, mb_release_id="mbid-dup"))
        with self.assertRaises(psycopg2.errors.UniqueViolation):
            db.seed_request(make_request_row(id=2, mb_release_id="mbid-dup"))

    def test_seed_request_same_id_reseed_is_an_update(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=1, mb_release_id="mbid-x", status="wanted"))
        db.seed_request(make_request_row(
            id=1, mb_release_id="mbid-x", status="unsearchable"))
        self.assertEqual(db.request(1)["status"], "unsearchable")

    def test_seed_request_allows_multiple_null_mb_release_ids(self):
        # PG UNIQUE permits any number of NULLs (Discogs-only rows).
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=1, mb_release_id=None, discogs_release_id="111"))
        db.seed_request(make_request_row(
            id=2, mb_release_id=None, discogs_release_id="222"))
        self.assertEqual(db.request(2)["discogs_release_id"], "222")

    def test_add_request_rejects_duplicate_mb_release_id(self):
        import psycopg2.errors

        db = FakePipelineDB()
        db.add_request("A", "B", "request", mb_release_id="mbid-dup")
        with self.assertRaises(psycopg2.errors.UniqueViolation):
            db.add_request("C", "D", "request", mb_release_id="mbid-dup")

    def test_add_request_allows_distinct_and_null_mb_release_ids(self):
        db = FakePipelineDB()
        db.add_request("A", "B", "request", mb_release_id="mbid-1")
        db.add_request("C", "D", "request", mb_release_id=None)
        rid = db.add_request("E", "F", "request", mb_release_id=None)
        self.assertEqual(db.request(rid)["artist_name"], "E")

    def test_reseed_cannot_steal_another_rows_mb_release_id(self):
        # exclude_id only exempts the row's OWN id — re-seeding id=1
        # with an mbid held by row 2 must still raise.
        import psycopg2.errors

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, mb_release_id="mbid-1"))
        db.seed_request(make_request_row(id=2, mb_release_id="mbid-2"))
        with self.assertRaises(psycopg2.errors.UniqueViolation):
            db.seed_request(make_request_row(id=1, mb_release_id="mbid-2"))

    def test_add_request_collides_with_seeded_row(self):
        # seed_request and add_request share one uniqueness check.
        import psycopg2.errors

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=7, mb_release_id="mbid-seeded"))
        with self.assertRaises(psycopg2.errors.UniqueViolation):
            db.add_request("A", "B", "request", mb_release_id="mbid-seeded")


class TestFakeRequestLifecycleWrites(unittest.TestCase):
    """Status transitions and attempt bookkeeping on ``album_requests``."""

    def test_request_creation_race_materializes_only_on_in_lock_lookup(self):
        db = FakePipelineDB()
        db.arm_request_creation_race(
            "race-release", status="imported",
        )

        self.assertIsNone(db.get_request_by_release_id("race-release"))
        winner = db.get_request_by_release_id("race-release")

        assert winner is not None
        self.assertEqual(winner["status"], "imported")
        again = db.get_request_by_release_id("race-release")
        assert again is not None
        self.assertEqual(
            again["id"],
            winner["id"],
        )

    def test_record_attempt_updates_retry_metadata(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="wanted"))

        db.record_attempt(42, "validation", expected_status="wanted")

        row = db.request(42)
        self.assertEqual(row["validation_attempts"], 1)
        self.assertIsNotNone(row["last_attempt_at"])
        self.assertIsNotNone(row["next_retry_after"])
        self.assertIsNotNone(row["updated_at"])
        self.assertEqual(db.recorded_attempts, [(42, "validation")])

    def test_record_attempt_rejects_processing_owner_even_when_status_matches(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            status="processing",
            active_automation_import_job_id=743,
        ))
        before = copy.deepcopy(db.request(42))

        self.assertFalse(db.record_attempt(
            42,
            "download",
            expected_status="processing",
        ))

        self.assertEqual(db.request(42), before)

    def test_set_downloading_sets_attempt_timestamps(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="wanted"))

        result = db.set_downloading(42, '{"enqueued_at":"2026-01-01T00:00:00+00:00"}')

        self.assertTrue(result)
        row = db.request(42)
        self.assertEqual(row["status"], "downloading")
        self.assertIsNotNone(row["last_attempt_at"])
        self.assertIsNotNone(row["updated_at"])
        self.assertEqual(
            row["active_download_state"],
            '{"enqueued_at":"2026-01-01T00:00:00+00:00"}',
        )
        self.assertEqual(db.status_history, [(42, "downloading")])

    def test_update_download_state_if_downloading_guards_status(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            status="downloading",
            active_download_state={
                "filetype": "old",
                "enqueued_at": "attempt-a",
                "files": [],
            },
        ))
        db.seed_request(make_request_row(
            id=43,
            status="wanted",
            active_download_state={
                "filetype": "old",
                "enqueued_at": "attempt-a",
                "files": [],
            },
        ))

        updated = db.update_download_state_if_downloading(
            42,
            '{"filetype":"flac","enqueued_at":"attempt-a","files":[]}',
            expected_enqueued_at="attempt-a",
        )
        blocked = db.update_download_state_if_downloading(
            43,
            '{"filetype":"mp3","enqueued_at":"attempt-a","files":[]}',
            expected_enqueued_at="attempt-a",
        )

        self.assertTrue(updated)
        self.assertFalse(blocked)
        self.assertEqual(
            db.request(42)["active_download_state"],
            {
                "filetype": "flac",
                "enqueued_at": "attempt-a",
                "files": [],
            },
        )
        self.assertEqual(
            db.request(43)["active_download_state"],
            {
                "filetype": "old",
                "enqueued_at": "attempt-a",
                "files": [],
            },
        )

    def test_update_download_state_if_downloading_rejects_stale_witness_unchanged(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            status="downloading",
            active_download_state={
                "filetype": "flac",
                "enqueued_at": "attempt-b",
                "files": [],
            },
        ))
        before = copy.deepcopy(db.request(42))

        updated = db.update_download_state_if_downloading(
            42,
            '{"filetype":"mp3","enqueued_at":"attempt-a","files":[]}',
            expected_enqueued_at="attempt-a",
        )

        self.assertFalse(updated)
        self.assertEqual(db.request(42), before)

    def test_set_update_download_state_error_raises_and_leaves_row_untouched(self):
        """Issue #564 review: the injection seam mirrors a psycopg2 error
        at the witnessed UPDATE, records the
        attempt, never mutates the row; other requests are unaffected."""
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=1, status="downloading",
            active_download_state={
                "filetype": "flac",
                "enqueued_at": "attempt-a",
                "files": [],
            }))
        db.seed_request(make_request_row(
            id=2,
            status="downloading",
            mb_release_id="mbid-2",
            active_download_state={
                "filetype": "flac",
                "enqueued_at": "attempt-b",
                "files": [],
            },
        ))
        boom = RuntimeError("UPDATE failed")
        db.set_update_download_state_error(1, boom)

        with self.assertRaises(RuntimeError):
            db.update_download_state_if_downloading(
                1,
                '{"filetype":"mp3","enqueued_at":"attempt-a","files":[]}',
                expected_enqueued_at="attempt-a",
            )

        # Row 1 untouched; the attempt is recorded.
        self.assertEqual(
            db.request(1)["active_download_state"],
            {
                "filetype": "flac",
                "enqueued_at": "attempt-a",
                "files": [],
            },
        )
        self.assertEqual(len(db.update_download_state_calls), 1)
        # Other requests still write normally.
        self.assertTrue(
            db.update_download_state_if_downloading(
                2,
                '{"filetype":"mp3","enqueued_at":"attempt-b","files":[]}',
                expected_enqueued_at="attempt-b",
            ))
        self.assertEqual(
            db.request(2)["active_download_state"],
            {
                "filetype": "mp3",
                "enqueued_at": "attempt-b",
                "files": [],
            },
        )

    def test_reset_downloading_to_wanted_guards_status_and_preserves_counters(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            status="downloading",
            active_download_state={"filetype": "flac"},
            download_attempts=3,
        ))
        db.seed_request(make_request_row(id=43, status="wanted"))

        reset = db.reset_downloading_to_wanted(42)
        blocked = db.reset_downloading_to_wanted(43)

        self.assertTrue(reset)
        self.assertFalse(blocked)
        self.assertEqual(db.request(42)["status"], "wanted")
        self.assertIsNone(db.request(42)["active_download_state"])
        self.assertEqual(db.request(42)["download_attempts"], 3)
        self.assertEqual(db.status_history, [(42, "wanted")])

    def test_wanted_resets_accept_explicit_previous_bitrate(self):
        """Fake parity: explicit history wins over derived old-min capture."""
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            status="unsearchable",
            min_bitrate=320,
            prev_min_bitrate=192,
        ))
        db.seed_request(make_request_row(
            id=43,
            status="downloading",
            min_bitrate=245,
            prev_min_bitrate=128,
        ))

        self.assertTrue(db.reset_to_wanted(
            42,
            expected_status="unsearchable",
            min_bitrate=224,
            prev_min_bitrate=256,
        ))
        self.assertTrue(db.reset_downloading_to_wanted(
            43,
            min_bitrate=192,
            prev_min_bitrate=None,
        ))

        self.assertEqual(db.request(42)["min_bitrate"], 224)
        self.assertEqual(db.request(42)["prev_min_bitrate"], 256)
        self.assertEqual(db.request(43)["min_bitrate"], 192)
        self.assertIsNone(db.request(43)["prev_min_bitrate"])

    def test_spectral_state_update_fields_apply(self):
        """The typed spectral payload lands through ``update_request_fields``
        — the production shape since the ``update_spectral_state`` wrapper
        (last reachable only from tests) was deleted with the dead
        measurement-side stamp writer."""
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42))

        update = RequestSpectralStateUpdate(
            current=SpectralMeasurement(grade="genuine", bitrate_kbps=None),
        )
        db.update_request_fields(42, **update.as_update_fields())

        row = db.request(42)
        self.assertEqual(row["current_spectral_grade"], "genuine")
        self.assertIsNone(row["current_spectral_bitrate"])

    def test_clear_on_disk_quality_fields_matches_real_db(self):
        """FakePipelineDB must mirror PipelineDB.clear_on_disk_quality_fields:
        zero current evidence + on-disk spectral + verified_lossless,
        preserve min_bitrate and last_download_spectral_* (those aren't
        on-disk state).
        """
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            min_bitrate=320,
            verified_lossless=True,
            current_spectral_grade="likely_transcode",
            current_spectral_bitrate=160,
            last_download_spectral_grade="suspect",
            last_download_spectral_bitrate=192,
            current_evidence_id=743,
        ))

        db.clear_on_disk_quality_fields(42)

        row = db.request(42)
        self.assertFalse(row["verified_lossless"])
        self.assertIsNone(row["current_spectral_grade"])
        self.assertIsNone(row["current_spectral_bitrate"])
        self.assertIsNone(row["current_evidence_id"])
        # min_bitrate preserved as baseline for next gate.
        self.assertEqual(row["min_bitrate"], 320)
        # Recent download's spectral is an audit trail, not on-disk state.
        self.assertEqual(row["last_download_spectral_grade"], "suspect")
        self.assertEqual(row["last_download_spectral_bitrate"], 192)

    def test_clear_on_disk_quality_fields_rejects_processing_owner(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            status="processing",
            active_automation_import_job_id=743,
            verified_lossless=True,
            current_spectral_grade="genuine",
            current_spectral_bitrate=245,
        ))
        before = copy.deepcopy(db.request(42))

        db.clear_on_disk_quality_fields(42)

        self.assertEqual(db.request(42), before)

    def test_set_marked_incomplete_mirrors_real_outcomes(self):
        """Issue #1241: the fake's outcome vocabulary and idempotence must
        mirror ``PipelineDB.set_marked_incomplete`` (real-PG round-trip in
        tests/test_pipeline_db.py::TestSetMarkedIncompleteRoundTrip)."""
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="imported"))
        db.seed_request(make_request_row(id=43, status="replaced"))

        self.assertEqual(db.set_marked_incomplete(999, marked=True), "not_found")
        self.assertEqual(db.set_marked_incomplete(43, marked=True), "replaced")

        self.assertEqual(db.set_marked_incomplete(42, marked=True), "marked")
        row = db.get_request(42)
        assert row is not None
        stamp = row["marked_incomplete_at"]
        self.assertIsNotNone(stamp)
        self.assertEqual(
            db.set_marked_incomplete(42, marked=True), "already_marked"
        )
        row = db.get_request(42)
        assert row is not None
        self.assertEqual(row["marked_incomplete_at"], stamp)

        self.assertEqual(db.set_marked_incomplete(42, marked=False), "cleared")
        row = db.get_request(42)
        assert row is not None
        self.assertIsNone(row["marked_incomplete_at"])
        self.assertEqual(
            db.set_marked_incomplete(42, marked=False), "already_clear"
        )

    def test_request_marked_incomplete_mirrors_the_narrow_read(self):
        """Issue #1241: the dispatch decision path's scalar read — a
        missing row reads as unmarked, never an error."""
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="imported"))
        self.assertFalse(db.request_marked_incomplete(42))
        self.assertFalse(db.request_marked_incomplete(999))
        db.set_marked_incomplete(42, marked=True)
        self.assertTrue(db.request_marked_incomplete(42))
        db.set_marked_incomplete(42, marked=False)
        self.assertFalse(db.request_marked_incomplete(42))


class TestFakeRequestMetadataGuards(unittest.TestCase):
    """The metadata writers' refusals: reserved fields, malformed values,
    lifecycle columns, and the empty-update compare-and-set.
    """

    def test_empty_request_field_update_is_a_read_only_cas(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=41, status="wanted"))
        db.seed_request(make_request_row(id=42, status="replaced"))
        active_before = copy.deepcopy(db.request(41))
        replaced_before = copy.deepcopy(db.request(42))

        self.assertTrue(db.update_request_fields(41))
        self.assertTrue(db.update_request_fields(
            41, expected_status="wanted",
        ))
        self.assertFalse(db.update_request_fields(
            41, expected_status="unsearchable",
        ))
        self.assertFalse(db.update_request_fields(42))
        self.assertFalse(db.update_request_fields(
            42, expected_status="replaced",
        ))
        self.assertFalse(db.update_request_fields(999))
        self.assertFalse(db.update_request_fields(
            999, expected_status="wanted",
        ))

        self.assertEqual(db.request(41), active_before)
        self.assertEqual(db.request(42), replaced_before)

    def test_metadata_update_rejects_every_reserved_field(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=41, status="wanted"))
        before = copy.deepcopy(db.request(41))

        for field in sorted(REQUEST_METADATA_RESERVED_FIELDS):
            with self.subTest(field=field):
                with self.assertRaisesRegex(
                    ValueError,
                    "reserved lifecycle/identity fields",
                ):
                    db.update_request_fields(41, **{
                        field: "replaced" if field == "status" else "smuggled",
                    })
                self.assertEqual(db.request(41), before)

        with self.assertRaises(ValueError):
            db.update_request_fields(41, status="unsearchable")
        self.assertEqual(db.request(41), before)

    def test_metadata_writers_reject_malformed_and_lifecycle_fields(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=41, status="wanted"))
        before = copy.deepcopy(db.request(41))

        for writer in (
            lambda: db.update_request_fields(
                41, **{"reasoning, status": "smuggled"},
            ),
            lambda: db.update_status(
                41, "imported", active_download_state="{}",
            ),
        ):
            with self.subTest(writer=writer):
                with self.assertRaises(ValueError):
                    writer()
                self.assertEqual(db.request(41), before)

    def test_reset_writers_reject_noncanonical_metadata(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=41, status="downloading"))
        before = copy.deepcopy(db.request(41))

        with self.assertRaises(ValueError):
            db.reset_to_wanted(41, reasoning="smuggled")
        with self.assertRaises(ValueError):
            db.reset_downloading_to_wanted(41, reasoning="smuggled")
        self.assertEqual(db.request(41), before)

    def test_spectral_fields_cannot_report_missing_or_replaced_success(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="replaced"))
        before = copy.deepcopy(db.request(42))
        fields = RequestSpectralStateUpdate(
            current=SpectralMeasurement(grade="genuine", bitrate_kbps=320),
        ).as_update_fields()

        self.assertFalse(db.update_request_fields(42, **fields))
        self.assertFalse(db.update_request_fields(999, **fields))
        self.assertEqual(db.request(42), before)


class TestFakeRequestMergeRekey(unittest.TestCase):
    """``update_request_release_for_merge`` and ``merge_rekey_collision``.

    Two of the four also assert on evidence rows the rekey carries with it.
    Both verbs belong to the requests cluster, which is what puts all four
    here rather than under evidence.
    """

    def test_merge_rekey_moves_only_an_owned_processing_row(self):
        """Fake mirror of ``PipelineDB.update_request_release_for_merge``."""
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=41,
            mb_release_id="merged-id",
            status="processing",
            active_automation_import_job_id=7,
        ))
        db.seed_request(make_request_row(id=42, mb_release_id="wanted-id"))

        self.assertTrue(db.update_request_release_for_merge(
            41,
            old_release_id="merged-id",
            new_release_id="survivor-id",
            expected_import_job_id=7,
        ))
        self.assertEqual(db.request(41)["mb_release_id"], "survivor-id")
        self.assertEqual(
            db.update_request_release_for_merge_calls,
            [(41, "merged-id", "survivor-id", 7)],
        )

        # A stale identity, a foreign owner, an unowned row, and a survivor
        # another request already holds all fail closed without writing.
        self.assertFalse(db.update_request_release_for_merge(
            41,
            old_release_id="merged-id",
            new_release_id="another-id",
            expected_import_job_id=7,
        ))
        self.assertFalse(db.update_request_release_for_merge(
            41,
            old_release_id="survivor-id",
            new_release_id="another-id",
            expected_import_job_id=8,
        ))
        self.assertFalse(db.update_request_release_for_merge(
            42,
            old_release_id="wanted-id",
            new_release_id="another-id",
            expected_import_job_id=7,
        ))
        self.assertFalse(db.update_request_release_for_merge(
            41,
            old_release_id="survivor-id",
            new_release_id="wanted-id",
            expected_import_job_id=7,
        ))
        self.assertEqual(db.request(41)["mb_release_id"], "survivor-id")
        self.assertEqual(db.request(42)["mb_release_id"], "wanted-id")

        for old_id, new_id in (
            ("survivor-id", "survivor-id"), ("", "x"), ("x", ""),
        ):
            with self.assertRaises(ValueError):
                db.update_request_release_for_merge(
                    41,
                    old_release_id=old_id,
                    new_release_id=new_id,
                    expected_import_job_id=7,
                )

    def test_merge_rekey_moves_the_requests_evidence_with_it(self):
        """Production moves both tables in one transaction; so does the fake."""
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=41,
            mb_release_id="merged-id",
            status="processing",
            active_automation_import_job_id=7,
        ))
        evidence = make_album_quality_evidence(mb_release_id="merged-id")
        db.upsert_album_quality_evidence(evidence)
        stored = db.find_album_quality_evidence(
            mb_release_id="merged-id",
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert stored is not None and stored.id is not None

        self.assertTrue(db.update_request_release_for_merge(
            41,
            old_release_id="merged-id",
            new_release_id="survivor-id",
            expected_import_job_id=7,
        ))

        self.assertIsNone(db.find_album_quality_evidence(
            mb_release_id="merged-id",
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        ))
        moved = db.find_album_quality_evidence(
            mb_release_id="survivor-id",
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert moved is not None
        self.assertEqual(moved.id, stored.id)
        by_id = db.load_album_quality_evidence_by_id(stored.id)
        assert by_id is not None
        self.assertEqual(by_id.mb_release_id, "survivor-id")

    def test_merge_rekey_refuses_a_fingerprint_collision_at_the_survivor(self):
        """Mirrors UNIQUE (mb_release_id, snapshot_fingerprint): nothing moves."""
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=41,
            mb_release_id="merged-id",
            status="processing",
            active_automation_import_job_id=7,
        ))
        for release_id in ("merged-id", "survivor-id"):
            db.upsert_album_quality_evidence(
                make_album_quality_evidence(mb_release_id=release_id),
            )
        fingerprint = make_album_quality_evidence(
            mb_release_id="merged-id",
        ).snapshot_fingerprint

        self.assertFalse(db.update_request_release_for_merge(
            41,
            old_release_id="merged-id",
            new_release_id="survivor-id",
            expected_import_job_id=7,
        ))

        self.assertEqual(db.request(41)["mb_release_id"], "merged-id")
        self.assertIsNotNone(db.find_album_quality_evidence(
            mb_release_id="merged-id", snapshot_fingerprint=fingerprint,
        ))
        self.assertIsNotNone(db.find_album_quality_evidence(
            mb_release_id="survivor-id", snapshot_fingerprint=fingerprint,
        ))

    def test_merge_rekey_collision_reports_both_documented_refusals(self):
        """The pre-check reads the same state the write refuses on (#1080).

        ``merge_rekey_collision`` exists so the seam never retags the shared
        Beets library for a rekey that is already refused. Fake and production
        must agree on both causes, and — critically — the fake's pre-check and
        its own write must not drift apart: every world this reports blocked,
        the write must refuse.
        """
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=41,
            mb_release_id="merged-id",
            status="processing",
            active_automation_import_job_id=7,
        ))

        clear = db.merge_rekey_collision(
            41, old_release_id="merged-id", new_release_id="survivor-id",
        )
        self.assertFalse(clear.blocked)
        self.assertIsNone(clear.rival_request_id)
        self.assertEqual(clear.colliding_fingerprints, ())
        self.assertEqual(clear.detail(), "")

        # A rival request at the survivor — production's UNIQUE(mb_release_id).
        # Any row counts, including a frozen ``replaced`` ancestor.
        db.seed_request(make_request_row(
            id=42, mb_release_id="survivor-id", status="replaced",
        ))
        rival = db.merge_rekey_collision(
            41, old_release_id="merged-id", new_release_id="survivor-id",
        )
        self.assertTrue(rival.blocked)
        self.assertEqual(rival.rival_request_id, 42)
        self.assertIn("42", rival.detail())
        # The write refuses the same world, so the pre-check never promises
        # something the write would then take back.
        self.assertFalse(db.update_request_release_for_merge(
            41,
            old_release_id="merged-id",
            new_release_id="survivor-id",
            expected_import_job_id=7,
        ))

        # An evidence fingerprint already at the survivor — production's
        # UNIQUE (mb_release_id, snapshot_fingerprint).
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=41,
            mb_release_id="merged-id",
            status="processing",
            active_automation_import_job_id=7,
        ))
        evidence = make_album_quality_evidence(mb_release_id="merged-id")
        for release_id in ("merged-id", "survivor-id"):
            db.upsert_album_quality_evidence(
                make_album_quality_evidence(mb_release_id=release_id),
            )
        collision = db.merge_rekey_collision(
            41, old_release_id="merged-id", new_release_id="survivor-id",
        )
        self.assertTrue(collision.blocked)
        self.assertIsNone(collision.rival_request_id)
        self.assertEqual(
            collision.colliding_fingerprints, (evidence.snapshot_fingerprint,),
        )
        self.assertIn("evidence already exists", collision.detail())
        self.assertFalse(db.update_request_release_for_merge(
            41,
            old_release_id="merged-id",
            new_release_id="survivor-id",
            expected_import_job_id=7,
        ))


class TestFakeRequestRows(unittest.TestCase):
    """Row creation, ordering and deletion, including the delete cascade.

    The cascade tests assert across download_log, search_plan, misc and
    evidence; ``delete_request`` itself is the requests cluster's.
    """

    def test_add_request_assigns_monotonic_id(self):
        db = FakePipelineDB()
        rid1 = db.add_request("Artist A", "Album A", source="request")
        rid2 = db.add_request("Artist B", "Album B", source="request")
        self.assertEqual((rid1, rid2), (1, 2))
        self.assertEqual(db.request(rid1)["artist_name"], "Artist A")
        self.assertEqual(db.request(rid2)["status"], "wanted")

    def test_add_request_seeds_full_row_shape(self):
        """Codex R7: rows must carry the DB-defaulted columns
        production readers index directly (``beets_distance``,
        ``*_attempts``, spectral + verified_lossless)
        so fake-backed tests don't raise ``KeyError`` where Postgres
        would return NULL/0."""
        db = FakePipelineDB()
        rid = db.add_request("X", "Y", source="request")
        row = db.request(rid)
        for key in (
            "beets_distance", "beets_scenario",
            "search_attempts", "download_attempts", "validation_attempts",
            "last_download_spectral_grade", "current_spectral_grade",
            "current_lossless_source_v0_probe_avg_bitrate",
            "verified_lossless", "min_bitrate", "prev_min_bitrate",
            "search_filetype_override", "target_format",
            "active_download_state",
        ):
            self.assertIn(key, row,
                          f"add_request row missing '{key}' — "
                          "production readers index it directly")
        self.assertEqual(row["search_attempts"], 0)
        self.assertEqual(row["download_attempts"], 0)
        self.assertEqual(row["validation_attempts"], 0)
        self.assertFalse(row["verified_lossless"])

    def test_add_request_coexists_with_seeded_ids(self):
        """Seeded ids must advance the auto-increment cursor so
        ``add_request`` cannot collide."""
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42))
        rid = db.add_request("X", "Y", source="request")
        self.assertEqual(rid, 43)

    def test_sort_mixes_seeded_iso_strings_and_added_datetimes(self):
        """``make_request_row`` seeds ISO strings, ``add_request``
        stores datetimes — the fake must normalise them so sorts
        don't raise ``TypeError`` on mixed input (codex R2)."""
        db = FakePipelineDB()
        # Seeded: ISO string timestamps.
        db.seed_request(make_request_row(id=1, status="wanted"))
        # Added: datetime timestamps.
        db.add_request("Artist", "Album", source="request")
        # Both of these would crash on ``str < datetime`` without
        # normalisation.
        rows = db.get_by_status("wanted")
        self.assertEqual(len(rows), 2)

    def test_delete_request_removes_row_and_tracks(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1))
        db.set_tracks(1, [{"track_number": 1, "title": "T"}])
        db.delete_request(1)
        self.assertNotIn(1, db._requests)
        self.assertEqual(db.get_tracks(1), [])

    def test_delete_request_cascades_to_child_tables(self):
        """Real SQL has ``ON DELETE CASCADE`` from album_requests to
        download_log, search_log, source_denylist, and the search plans with
        their items. The fake must prune those too so tests cannot observe an
        impossible state where orphaned child rows survive their parent
        (codex R2).

        The plan arm is asserted here as well as from the plan side in
        ``tests/test_fakes_search_plan.py``. The cascade code lives in the
        requests cluster, so without this a mutant there survives everything
        selection pulls for a change to that file (#1313 review runner, R3).
        """
        from lib.pipeline_db import SearchPlanItemInput

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1))
        db.seed_request(make_request_row(id=2, mb_release_id="mb-survivor"))
        db.log_download(1, outcome="success")
        db.log_download(2, outcome="success")
        db.log_search(1, outcome="found")
        db.log_search(2, outcome="no_match")
        db.add_denylist(1, "badguy")
        db.add_denylist(2, "other")
        doomed_plan = db.create_successful_search_plan(
            request_id=1, generator_id="g1",
            items=[SearchPlanItemInput(ordinal=0, strategy="default", query="Q0")])
        kept_plan = db.create_successful_search_plan(
            request_id=2, generator_id="g1",
            items=[SearchPlanItemInput(ordinal=0, strategy="default", query="Q1")])

        db.delete_request(1)

        self.assertEqual([e.request_id for e in db.download_logs], [2])
        self.assertEqual([e.request_id for e in db.search_logs], [2])
        self.assertEqual([e.request_id for e in db.denylist], [2])
        self.assertEqual(list(db.search_plans), [kept_plan])
        self.assertEqual(
            {it.plan_id for it in db.search_plan_items.values()}, {kept_plan})
        self.assertNotEqual(doomed_plan, kept_plan)

    def test_delete_request_does_not_cascade_evidence_post_021(self):
        """Migration 021: evidence is content-addressed. Deleting a request
        no longer removes evidence rows — addressing FKs go ``ON DELETE SET
        NULL`` so the row survives the parent's removal.
        """
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1))
        log_id = db.log_download(1, outcome="rejected")
        evidence = make_album_quality_evidence(mb_release_id="mb-delete-1")
        db.upsert_album_quality_evidence(evidence)
        persisted = db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert persisted is not None and persisted.id is not None
        db.set_download_log_candidate_evidence(log_id, persisted.id)

        db.delete_request(1)

        # Evidence rows survive; the parent and its child download_log are
        # gone via the cascade rules earlier in delete_request.
        self.assertIsNotNone(db.load_album_quality_evidence_by_id(persisted.id))


class TestFakeRequestReads(unittest.TestCase):
    """The request read models: get_wanted, status filters and counts,
    artist lookup, the long-tail cohort, and the #426 recency window and
    search mirrors.
    """

    def test_get_downloading(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="downloading"))
        db.seed_request(make_request_row(id=2, status="wanted"))
        db.seed_request(make_request_row(id=3, status="downloading"))

        rows = db.get_downloading()
        self.assertEqual(len(rows), 2)
        ids = {r["id"] for r in rows}
        self.assertEqual(ids, {1, 3})

    def test_get_wanted_does_not_prioritize_zero_attempts(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="wanted",
                                          search_attempts=5))
        db.seed_request(make_request_row(id=2, status="wanted",
                                          search_attempts=0))
        db.seed_request(make_request_row(id=3, status="imported"))
        rows = db.get_wanted()
        self.assertEqual([r["id"] for r in rows], [1, 2])
        self.assertEqual(
            [r["id"] for r in db.get_wanted(limit=1)], [1])

    def test_get_wanted_skips_albums_inside_retry_window(self):
        db = FakePipelineDB()
        future = datetime.now(UTC) + timedelta(hours=1)
        db.seed_request(make_request_row(
            id=1, status="wanted", next_retry_after=future))
        db.seed_request(make_request_row(id=2, status="wanted"))
        rows = db.get_wanted()
        self.assertEqual([r["id"] for r in rows], [2])

    def test_get_wanted_tie_break_is_set_not_order(self):
        """The real DB randomises order; callers assert set membership."""
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=1, status="wanted", search_attempts=0))
        db.seed_request(make_request_row(
            id=2, status="wanted", search_attempts=0))
        db.seed_request(make_request_row(
            id=3, status="wanted", search_attempts=0))
        rows = db.get_wanted()
        self.assertEqual({r["id"] for r in rows}, {1, 2, 3})

    def test_get_by_status_sorts_by_created_at(self):
        db = FakePipelineDB()
        now = datetime.now(UTC)
        db.seed_request(make_request_row(
            id=1, status="wanted", created_at=now + timedelta(seconds=2)))
        db.seed_request(make_request_row(
            id=2, status="wanted", created_at=now))
        rows = db.get_by_status("wanted")
        self.assertEqual([r["id"] for r in rows], [2, 1])

    def test_get_by_status_recent_window(self):
        db = FakePipelineDB()
        ids = []
        for i in range(3):
            ids.append(db.add_request(
                artist_name=f"A{i}", album_title=f"T{i}", source="request",
                mb_release_id=f"win-{i}", status="imported"))
        db.update_request_fields(ids[0], reasoning="touched")

        rows = db.get_by_status("imported", limit=2, newest_first=True)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["id"], ids[0])
        # Default shape unchanged.
        self.assertEqual(len(db.get_by_status("imported")), 3)

    def test_count_by_status(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="wanted"))
        db.seed_request(make_request_row(id=2, status="wanted"))
        db.seed_request(make_request_row(id=3, status="imported"))
        self.assertEqual(
            db.count_by_status(), {"wanted": 2, "imported": 1})

    def test_count_by_status_preserves_none_bucket(self):
        """Real SQL ``GROUP BY status`` keeps NULL as its own key; the
        fake must not collapse it to an empty string."""
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status=None))
        db.seed_request(make_request_row(id=2, status="wanted"))
        self.assertEqual(db.count_by_status(), {None: 1, "wanted": 1})

    def test_list_requests_by_artist_prefers_mb_artist_id_and_legacy_fallback(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=1,
            artist_name="Test Artist",
            album_title="Exact MBID",
            mb_artist_id="artist-1234-uuid",
        ))
        db.seed_request(make_request_row(
            id=2,
            artist_name="Test Artist",
            album_title="Legacy Name Match",
            mb_artist_id=None,
        ))
        db.seed_request(make_request_row(
            id=3,
            artist_name="Test Artist",
            album_title="Other MBID",
            mb_artist_id="other-artist-uuid",
        ))

        rows = db.list_requests_by_artist("Test Artist", "artist-1234-uuid")

        self.assertEqual([row["id"] for row in rows], [1, 2])

    def test_list_requests_by_artist_name_only_matches_substring(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=1,
            artist_name="The National",
            album_title="Boxer",
            year=2007,
        ))
        db.seed_request(make_request_row(
            id=2,
            artist_name="The National",
            album_title="Sleep Well Beast",
            year=2017,
        ))
        db.seed_request(make_request_row(
            id=3,
            artist_name="Nation of Language",
            album_title="Introduction, Presence",
            year=2020,
        ))

        rows = db.list_requests_by_artist("The National")

        self.assertEqual([row["id"] for row in rows], [1, 2])

    def test_search_requests_matches_artist_and_album(self):
        db = FakePipelineDB()
        db.add_request(
            artist_name="The Mountain Goats", album_title="Tallahassee",
            source="request", mb_release_id="f-sr-1", status="imported")
        db.add_request(
            artist_name="Goat", album_title="World Music",
            source="request", mb_release_id="f-sr-2", status="wanted")

        self.assertEqual(
            [r["mb_release_id"] for r in db.search_requests("mountain")],
            ["f-sr-1"])
        self.assertEqual(
            [r["mb_release_id"] for r in db.search_requests("world mus")],
            ["f-sr-2"])
        self.assertEqual(
            {r["mb_release_id"] for r in db.search_requests("goat")},
            {"f-sr-1", "f-sr-2"})
        self.assertEqual(db.search_requests("  "), [])
        self.assertEqual(
            [r["mb_release_id"]
             for r in db.search_requests("goat", status="wanted")],
            ["f-sr-2"])

    def test_get_long_tail_cohort_returns_only_wanted_stamped(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=1, status="wanted", mb_release_id="rel-1"))
        db.seed_request(make_request_row(
            id=2, status="imported", mb_release_id="rel-2"))
        db.seed_request(make_request_row(
            id=3, status="wanted", mb_release_id="rel-3"))
        # Row 3 has an in-flight youtube rescue.
        db.insert_youtube_running(
            request_id=3, browse_id="MPREb_x", audio_playlist_id=None,
            yt_url="https://music.youtube.com/playlist?list=x",
            expected_track_count=10,
        )
        rows = db.get_long_tail_cohort()
        self.assertEqual([r["id"] for r in rows], [1, 3])
        by_id = {r["id"]: r for r in rows}
        self.assertFalse(by_id[1]["in_flight_rescue"])
        self.assertTrue(by_id[3]["in_flight_rescue"])
        # Projection is narrow — must not carry the full request row.
        self.assertNotIn("reasoning", by_id[1])
        self.assertIn("target_format", by_id[1])

    def test_get_long_tail_request_single_id(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=5, status="wanted", mb_release_id="rel-5"))
        db.seed_request(make_request_row(
            id=6, status="imported", mb_release_id="rel-6"))
        row = db.get_long_tail_request(5)
        assert row is not None
        self.assertEqual(row["id"], 5)
        self.assertFalse(row["in_flight_rescue"])
        # Non-wanted and missing ids return None.
        self.assertIsNone(db.get_long_tail_request(6))
        self.assertIsNone(db.get_long_tail_request(999))


if __name__ == "__main__":
    unittest.main()
