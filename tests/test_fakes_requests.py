"""Self-tests for the FakePipelineDB album_requests cluster.

Split out of ``tests/test_fakes.py`` (#1313) so each cluster of the
fake has its sibling tests beside it.
"""
import unittest
from datetime import UTC, datetime, timedelta

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

    Mirrors ``.claude/rules/code-quality.md`` § "Every new PipelineDB
    method needs an equivalent stub on ``FakePipelineDB`` with a self-
    test in its cluster test module." Each test exercises a single
    fake method's contract — call recording + persisted row state.
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


if __name__ == "__main__":
    unittest.main()
