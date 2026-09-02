"""Self-tests for the FakePipelineDB misc cluster: denylist, cooldowns, triage, field resolutions, bad-audio hashes, and the slskd event cursor.

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


class TestFakePipelineDBFieldResolutions(unittest.TestCase):
    """FakePipelineDB mirrors the ``album_request_field_resolutions`` UPSERT
    semantics. Tests asserting on side-table state use this fake; the real
    PipelineDB integration is exercised in ``tests/test_pipeline_db.py``.
    """

    def setUp(self) -> None:
        self.db = FakePipelineDB()
        self.db.seed_request(make_request_row(
            id=42,
            status="wanted",
            mb_release_id="field-resolution-parent",
        ))

    def test_first_call_creates_row_with_attempts_one(self):
        db = self.db
        db.record_field_resolution(
            request_id=42,
            field_name="release_group_year",
            status="resolved",
            reason_code=None,
        )
        row = db.get_field_resolution(42, "release_group_year")
        assert row is not None
        self.assertEqual(row["status"], "resolved")
        self.assertIsNone(row["reason_code"])
        self.assertEqual(row["attempts"], 1)
        self.assertEqual(row["request_id"], 42)
        self.assertEqual(row["field_name"], "release_group_year")

    def test_re_upsert_increments_attempts_and_updates_status(self):
        db = self.db
        db.record_field_resolution(
            request_id=42, field_name="release_group_year",
            status="unresolved_mirror_unavailable", reason_code="URLError",
        )
        db.record_field_resolution(
            request_id=42, field_name="release_group_year",
            status="resolved", reason_code=None,
        )
        row = db.get_field_resolution(42, "release_group_year")
        assert row is not None
        self.assertEqual(row["status"], "resolved")
        self.assertIsNone(row["reason_code"])
        self.assertEqual(row["attempts"], 2)
        # Only one row -- not duplicated.
        self.assertEqual(len(db.field_resolutions), 1)

    def test_different_fields_get_distinct_rows(self):
        db = self.db
        db.record_field_resolution(
            request_id=42, field_name="release_group_year",
            status="resolved", reason_code=None,
        )
        db.record_field_resolution(
            request_id=42, field_name="catalog_number",
            status="unresolved_404", reason_code="http_404",
        )
        self.assertEqual(len(db.field_resolutions), 2)
        self.assertEqual(
            db.get_field_resolution(42, "release_group_year")["status"],  # type: ignore[index]
            "resolved",
        )
        self.assertEqual(
            db.get_field_resolution(42, "catalog_number")["status"],  # type: ignore[index]
            "unresolved_404",
        )

    def test_get_field_resolution_returns_none_when_absent(self):
        self.assertIsNone(
            self.db.get_field_resolution(42, "release_group_year"),
        )

    def test_missing_or_replaced_parent_rejects_write(self):
        self.db.seed_request(make_request_row(
            id=43,
            status="replaced",
            mb_release_id="field-resolution-frozen-parent",
        ))

        self.assertFalse(self.db.record_field_resolution(
            request_id=999,
            field_name="release_group_year",
            status="resolved",
            reason_code=None,
        ))
        self.assertFalse(self.db.record_field_resolution(
            request_id=43,
            field_name="release_group_year",
            status="resolved",
            reason_code=None,
        ))
        self.assertEqual(self.db.field_resolutions, {})


class TestFakePipelineDBTriage(unittest.TestCase):
    """Each of the four triage-bound methods on ``FakePipelineDB`` has a
    self-test so the cohort path stays trustworthy when the production
    SQL is updated. The N+1 guard test lives in
    ``tests/test_triage_service.py``.
    """

    def _seed_two_requests(self) -> FakePipelineDB:
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=1, artist_name="Artist One", album_title="Album One",
            unfindable_category="artist_absent",
        ))
        db.seed_request(make_request_row(
            id=2, artist_name="Artist Two", album_title="Album Two",
        ))
        return db

    def test_list_triage_page_filter_all(self):
        from lib.triage_service import parse_filter
        db = self._seed_two_requests()
        rows = db.list_triage_page(
            filter_spec=parse_filter("all"),
            page_size=10,
            after_request_id=None,
        )
        self.assertEqual([r["id"] for r in rows], [1, 2])
        self.assertEqual(db.query_counts["list_triage_page"], 1)

    def test_list_triage_page_filter_unfindable(self):
        from lib.triage_service import parse_filter
        db = self._seed_two_requests()
        rows = db.list_triage_page(
            filter_spec=parse_filter("unfindable"),
            page_size=10,
            after_request_id=None,
        )
        self.assertEqual([r["id"] for r in rows], [1])

    def test_list_triage_page_keyset_pagination(self):
        from lib.triage_service import parse_filter
        db = FakePipelineDB()
        for i in range(1, 6):
            db.seed_request(make_request_row(id=i))
        page = db.list_triage_page(
            filter_spec=parse_filter("all"),
            page_size=2,
            after_request_id=2,
        )
        self.assertEqual([r["id"] for r in page], [3, 4])

    def test_list_triage_page_filter_data_quality(self):
        """The EXISTS-join branch over ``album_request_field_resolutions``.

        Three rows: (1) has an unresolved field-resolution, (2) has only
        a resolved-status row, (3) has none. Only row 1 must match the
        bare ``data_quality`` filter; narrowing on field_name / status_code
        / reason_code further restricts the cohort. Mirrors the production
        SQL contract — same shape would fail if the fake forgot a sub-filter.
        """
        from lib.triage_service import parse_filter
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1))
        db.seed_request(make_request_row(id=2))
        db.seed_request(make_request_row(id=3))
        db.record_field_resolution(
            request_id=1, field_name="release_group_year",
            status="unresolved_4xx_client", reason_code="http_400",
        )
        db.record_field_resolution(
            request_id=2, field_name="catalog_number",
            status="resolved", reason_code=None,
        )

        # Bare data_quality — only request 1 (has unresolved_*).
        rows = db.list_triage_page(
            filter_spec=parse_filter("data_quality"),
            page_size=10,
            after_request_id=None,
        )
        self.assertEqual([r["id"] for r in rows], [1])

        # Narrow on field_name — release_group_year matches request 1.
        rows = db.list_triage_page(
            filter_spec=parse_filter("data_quality:release_group_year"),
            page_size=10,
            after_request_id=None,
        )
        self.assertEqual([r["id"] for r in rows], [1])

        # Narrow on status — unresolved_4xx_client matches request 1.
        rows = db.list_triage_page(
            filter_spec=parse_filter(
                "data_quality:status=unresolved_4xx_client",
            ),
            page_size=10,
            after_request_id=None,
        )
        self.assertEqual([r["id"] for r in rows], [1])

        # Narrow on reason_code — http_400 matches request 1.
        rows = db.list_triage_page(
            filter_spec=parse_filter("data_quality:reason=http_400"),
            page_size=10,
            after_request_id=None,
        )
        self.assertEqual([r["id"] for r in rows], [1])

        # Negative narrow — a mismatched reason_code excludes request 1.
        rows = db.list_triage_page(
            filter_spec=parse_filter("data_quality:reason=http_999"),
            page_size=10,
            after_request_id=None,
        )
        self.assertEqual(rows, [])

    def test_list_triage_page_filter_search_not_converting(self):
        """The join against ``request_search_summary`` excludes rows with
        no search log entries AND rows with any found outcome.

        Three rows: (1) 3 searches all rejected → matches; (2) one found
        outcome → excluded; (3) no searches → excluded.
        """
        from lib.triage_service import parse_filter
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1))
        db.seed_request(make_request_row(id=2))
        db.seed_request(make_request_row(id=3))
        for _ in range(3):
            db.log_search(
                request_id=1, query="q", result_count=10, outcome="rejected",
            )
        db.log_search(
            request_id=2, query="q", result_count=10, outcome="found",
        )
        # Request 3: no search log rows.

        rows = db.list_triage_page(
            filter_spec=parse_filter("search_not_converting"),
            page_size=10,
            after_request_id=None,
        )
        self.assertEqual([r["id"] for r in rows], [1])

    def test_get_field_resolutions_for_requests_groups_by_id(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1))
        db.seed_request(make_request_row(id=2))
        db.record_field_resolution(
            request_id=1, field_name="release_group_year",
            status="resolved", reason_code=None,
        )
        db.record_field_resolution(
            request_id=1, field_name="catalog_number",
            status="unresolved_404", reason_code="http_404",
        )
        db.record_field_resolution(
            request_id=2, field_name="release_group_year",
            status="resolved", reason_code=None,
        )
        out = db.get_field_resolutions_for_requests([1, 2])
        self.assertEqual(len(out[1]), 2)
        self.assertEqual(len(out[2]), 1)
        self.assertEqual(db.query_counts["get_field_resolutions_for_requests"], 1)

    def test_get_field_resolutions_for_requests_empty_input(self):
        db = FakePipelineDB()
        self.assertEqual(db.get_field_resolutions_for_requests([]), {})

    def test_get_search_summaries_for_requests_emits_zero_groups_only_when_present(
        self,
    ):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1))
        db.seed_request(make_request_row(id=2))
        db.log_search(
            request_id=1, query="q", result_count=5, outcome="found",
        )
        out = db.get_search_summaries_for_requests([1, 2])
        # Only request 1 has search_log rows; request 2 has no row in
        # the view (mirrors GROUP BY excluding empty groups).
        self.assertIn(1, out)
        self.assertNotIn(2, out)
        self.assertEqual(out[1]["total_searches"], 1)
        self.assertEqual(out[1]["found_count"], 1)
        self.assertEqual(db.query_counts["get_search_summaries_for_requests"], 1)

    def test_get_recent_search_log_for_requests_bounded_per_request(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1))
        for i in range(20):
            db.log_search(
                request_id=1, query=f"q{i}", result_count=i, outcome="error",
            )
        out = db.get_recent_search_log_for_requests([1], per_request_limit=5)
        self.assertEqual(len(out[1]), 5)
        # Newest first — last logged query is "q19".
        self.assertEqual(out[1][0]["query"], "q19")
        self.assertEqual(db.query_counts["get_recent_search_log_for_requests"], 1)


class TestFakeBadAudioHashes(unittest.TestCase):
    """Self-tests for the bad_audio_hashes fake methods (plan U2)."""

    def _hash(self, n: int) -> bytes:
        return bytes([n]) * 32

    def test_add_bad_audio_hashes_returns_count_for_fresh_inserts(self):
        from lib.pipeline_db import BadAudioHashInput
        db = FakePipelineDB()
        inputs = [
            BadAudioHashInput(hash_value=self._hash(1), audio_format="flac"),
            BadAudioHashInput(hash_value=self._hash(2), audio_format="mp3"),
            BadAudioHashInput(hash_value=self._hash(3), audio_format="m4a"),
        ]
        n = db.add_bad_audio_hashes(42, "H@rco", "bad rip", inputs)
        self.assertEqual(n, 3)
        self.assertEqual(len(db.bad_audio_hashes), 3)
        self.assertEqual(db.bad_audio_hashes[0].request_id, 42)
        self.assertEqual(db.bad_audio_hashes[0].reported_username, "H@rco")
        self.assertEqual(db.bad_audio_hashes[0].reason, "bad rip")
        # Auto-incrementing ids
        ids = [r.id for r in db.bad_audio_hashes]
        self.assertEqual(ids, [1, 2, 3])

    def test_add_bad_audio_hashes_returns_zero_on_full_duplicate(self):
        from lib.pipeline_db import BadAudioHashInput
        db = FakePipelineDB()
        inputs = [
            BadAudioHashInput(hash_value=self._hash(1), audio_format="flac"),
            BadAudioHashInput(hash_value=self._hash(2), audio_format="mp3"),
        ]
        first = db.add_bad_audio_hashes(42, "H@rco", "bad rip", inputs)
        second = db.add_bad_audio_hashes(99, "OtherUser", "duplicate", inputs)
        self.assertEqual(first, 2)
        self.assertEqual(second, 0)
        self.assertEqual(len(db.bad_audio_hashes), 2)
        # First-writer-wins on attribution
        self.assertEqual(db.bad_audio_hashes[0].request_id, 42)
        self.assertEqual(db.bad_audio_hashes[0].reported_username, "H@rco")

    def test_add_bad_audio_hashes_partial_overlap(self):
        from lib.pipeline_db import BadAudioHashInput
        db = FakePipelineDB()
        first_batch = [
            BadAudioHashInput(hash_value=self._hash(1), audio_format="flac"),
            BadAudioHashInput(hash_value=self._hash(2), audio_format="flac"),
        ]
        db.add_bad_audio_hashes(42, "H@rco", "bad rip", first_batch)
        second_batch = [
            BadAudioHashInput(hash_value=self._hash(2), audio_format="flac"),
            BadAudioHashInput(hash_value=self._hash(3), audio_format="flac"),
        ]
        n = db.add_bad_audio_hashes(43, "OtherUser", "bad rip", second_batch)
        # Only the genuinely-new (3, flac) row inserted
        self.assertEqual(n, 1)
        self.assertEqual(len(db.bad_audio_hashes), 3)

    def test_add_bad_audio_hashes_empty_list_is_zero(self):
        db = FakePipelineDB()
        n = db.add_bad_audio_hashes(42, "u", "r", [])
        self.assertEqual(n, 0)

    def test_add_bad_audio_hashes_same_hash_different_format_both_inserted(self):
        from lib.pipeline_db import BadAudioHashInput
        db = FakePipelineDB()
        inputs = [
            BadAudioHashInput(hash_value=self._hash(1), audio_format="flac"),
            BadAudioHashInput(hash_value=self._hash(1), audio_format="mp3"),
        ]
        n = db.add_bad_audio_hashes(42, "u", "r", inputs)
        self.assertEqual(n, 2)

    def test_lookup_bad_audio_hash_hits_when_present(self):
        from lib.pipeline_db import BadAudioHashInput
        db = FakePipelineDB()
        db.add_bad_audio_hashes(
            42, "u", "r",
            [BadAudioHashInput(hash_value=self._hash(7), audio_format="flac")],
        )
        row = db.lookup_bad_audio_hash(self._hash(7), "flac")
        assert row is not None
        self.assertEqual(row.hash_value, self._hash(7))
        self.assertEqual(row.audio_format, "flac")
        self.assertEqual(row.request_id, 42)
        self.assertEqual(row.reported_username, "u")

    def test_lookup_bad_audio_hash_miss_returns_none(self):
        db = FakePipelineDB()
        self.assertIsNone(db.lookup_bad_audio_hash(self._hash(9), "flac"))

    def test_lookup_bad_audio_hash_format_must_match(self):
        from lib.pipeline_db import BadAudioHashInput
        db = FakePipelineDB()
        db.add_bad_audio_hashes(
            42, "u", "r",
            [BadAudioHashInput(hash_value=self._hash(7), audio_format="flac")],
        )
        # Same hash, different format → miss
        self.assertIsNone(db.lookup_bad_audio_hash(self._hash(7), "mp3"))
        # Same format, different hash → miss
        self.assertIsNone(db.lookup_bad_audio_hash(self._hash(8), "flac"))

    def test_has_any_bad_audio_hashes_false_on_fresh_fake(self):
        db = FakePipelineDB()
        self.assertFalse(db.has_any_bad_audio_hashes())

    def test_has_any_bad_audio_hashes_true_after_one_insert(self):
        from lib.pipeline_db import BadAudioHashInput
        db = FakePipelineDB()
        db.add_bad_audio_hashes(
            42, None, None,
            [BadAudioHashInput(hash_value=self._hash(1), audio_format="flac")],
        )
        self.assertTrue(db.has_any_bad_audio_hashes())


class TestFakePipelineDBSlskdEventCursor(unittest.TestCase):
    """Self-tests for the slskd event cursor stubs (issue #146)."""

    def test_cursor_starts_absent(self):
        db = FakePipelineDB()
        self.assertIsNone(db.get_slskd_event_cursor())

    def test_upsert_round_trip_and_replace(self):
        db = FakePipelineDB()
        db.upsert_slskd_event_cursor("ev-1", "2026-07-01T00:00:00.0000000Z")
        cursor = db.get_slskd_event_cursor()
        assert cursor is not None
        self.assertEqual(cursor["last_event_id"], "ev-1")
        self.assertEqual(
            cursor["last_event_timestamp"], "2026-07-01T00:00:00.0000000Z")
        self.assertIsNotNone(cursor["updated_at"])

        db.upsert_slskd_event_cursor("ev-2", "2026-07-02T00:00:00.0000000Z")
        cursor = db.get_slskd_event_cursor()
        assert cursor is not None
        self.assertEqual(cursor["last_event_id"], "ev-2")

    def test_returned_cursor_is_a_copy(self):
        db = FakePipelineDB()
        db.upsert_slskd_event_cursor("ev-1", "2026-07-01T00:00:00.0000000Z")
        first = db.get_slskd_event_cursor()
        assert first is not None
        first["last_event_id"] = "mutated"
        second = db.get_slskd_event_cursor()
        assert second is not None
        self.assertEqual(second["last_event_id"], "ev-1")


class TestFakeRequestTracks(unittest.TestCase):
    """The per-request track list and the track-artist aligner."""

    def test_tracks_round_trip_and_count(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="wanted"))
        db.set_tracks(1, [
            {"track_number": 2, "title": "Second"},
            {"track_number": 1, "title": "First"},
        ])
        rows = db.get_tracks(1)
        self.assertEqual([t["track_number"] for t in rows], [1, 2])
        self.assertEqual(db.get_track_counts([1, 99]), {1: 2})
        # Per-track artist defaults to None when set_tracks input omits it
        # (matches real DB: track_artist column defaults to NULL).
        self.assertEqual([r["track_artist"] for r in rows], [None, None])

    def test_set_tracks_persists_inline_track_artist(self):
        """``set_tracks`` should forward ``track_artist`` when present in
        the upstream payload (e.g. discogs adapter passes per-track
        artists directly)."""
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="wanted"))
        db.set_tracks(1, [
            {"track_number": 1, "title": "T1", "track_artist": "Artist X"},
            {"track_number": 2, "title": "T2", "track_artist": None},
        ])
        rows = db.get_tracks(1)
        self.assertEqual(
            [r["track_artist"] for r in rows], ["Artist X", None],
        )

    def test_update_track_artists_aligns_by_disc_track_order(self):
        """``update_track_artists`` mirrors real DB ordering: rows are
        sorted by (disc, track) and the input list zips against that
        order — so the resolver's per-track output, which sorts the
        same way, lines up."""
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="wanted"))
        db.set_tracks(1, [
            {"track_number": 2, "title": "Second", "disc_number": 1},
            {"track_number": 1, "title": "First", "disc_number": 1},
            {"track_number": 1, "title": "Disc2-T1", "disc_number": 2},
        ])
        db.update_track_artists(1, ["A", "B", "C"])
        rows = db.get_tracks(1)
        # (disc=1, track=1)→A, (disc=1, track=2)→B, (disc=2, track=1)→C
        self.assertEqual(
            [r["track_artist"] for r in rows], ["A", "B", "C"],
        )

    def test_update_track_artists_tolerates_length_mismatch(self):
        """Fewer entries: trailing rows keep existing value. More
        entries: extras silently dropped. Same shape as real DB."""
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="wanted"))
        db.set_tracks(1, [
            {"track_number": 1, "title": "T1", "track_artist": "Pre"},
            {"track_number": 2, "title": "T2", "track_artist": "Pre"},
            {"track_number": 3, "title": "T3", "track_artist": "Pre"},
        ])
        # Fewer
        db.update_track_artists(1, ["A"])
        rows = db.get_tracks(1)
        self.assertEqual(
            [r["track_artist"] for r in rows], ["A", "Pre", "Pre"],
        )
        # More — extras silently dropped, others overwritten
        db.update_track_artists(1, ["X", "Y", "Z", "EXTRA"])
        rows = db.get_tracks(1)
        self.assertEqual(
            [r["track_artist"] for r in rows], ["X", "Y", "Z"],
        )

    def test_update_track_artists_empty_input_is_noop(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="wanted"))
        db.set_tracks(1, [
            {"track_number": 1, "title": "T1", "track_artist": "Pre"},
        ])
        db.update_track_artists(1, [])
        self.assertEqual(
            db.get_tracks(1)[0]["track_artist"], "Pre",
        )


class TestFakeDenylistAndCooldowns(unittest.TestCase):
    """The denylist upsert and the per-user cooldown filter."""

    def test_add_denylist_ignores_duplicate_like_postgres(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42))

        db.add_denylist(42, "peer", "first")
        db.add_denylist(42, "peer", "second")

        self.assertEqual(
            db.get_denylisted_users(42),
            [{"username": "peer", "reason": "first", "created_at": None}],
        )

    def test_user_cooldowns_upsert_and_filter(self):
        db = FakePipelineDB()
        now = datetime.now(UTC)
        db.add_cooldown("alice", now + timedelta(days=3), reason="x")
        db.add_cooldown("bob", now - timedelta(days=1), reason="expired")
        # Upsert — second call on alice replaces cooldown_until/reason.
        db.add_cooldown("alice", now + timedelta(days=7), reason="y")

        active = db.get_cooled_down_users()
        self.assertEqual(active, ["alice"])
        # Upsert replaced rather than duplicated alice's row.
        self.assertEqual(len(db.user_cooldowns), 2)
        self.assertEqual(db.user_cooldowns["alice"].reason, "y")


if __name__ == "__main__":
    unittest.main()
