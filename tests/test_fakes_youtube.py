"""Self-tests for the FakePipelineDB YouTube cluster.

Split out of ``tests/test_fakes.py`` (#1313) so each cluster of the
fake has its sibling tests beside it.
"""
import unittest
from typing import Any, cast

import msgspec

from lib.pipeline_db import (
    PersistedDistance,
    PersistedTrack,
    PersistedYoutubeRow,
)
from tests.fakes import (
    FakePipelineDB,
)


class TestFakePipelineDBYoutubeAlbumMappings(unittest.TestCase):
    """Self-test for FakePipelineDB youtube_album_mappings CRUD (U4).

    Mirrors the real ``PipelineDB.get_youtube_album_mapping`` /
    ``upsert_youtube_album_mapping`` surface. Backing store is keyed by
    ``(release_group_identifier, source)`` so a single MB release-group
    or Discogs master maps to the full per-sibling matrix the resolver
    produced.
    """

    def _row(self, **overrides: Any) -> PersistedYoutubeRow:
        fields: dict[str, Any] = {
            "yt_browse_id": "MPREb_abc",
            "yt_audio_playlist_id": "OLAK5uy_abc",
            "yt_url": "https://music.youtube.com/playlist?list=OLAK5uy_abc",
            "yt_year": 2020,
            "yt_track_count": 10,
            "yt_tracks": [
                PersistedTrack(
                    title="Track 1", video_id="v1", length_seconds=200,
                    track_number=1, disc_number=1,
                    artists=[{"name": "Artist"}],
                ),
            ],
            "distances": [
                PersistedDistance(mbid="mb-1", distance=0.05),
            ],
        }
        fields.update(overrides)
        return PersistedYoutubeRow(**fields)

    def test_get_returns_none_when_pair_never_resolved(self):
        # Distinction matters: ``None`` = "never resolved" (cache MISS),
        # ``[]`` = "resolved to empty matrix" (cache HIT). See finding #3.
        db = FakePipelineDB()
        self.assertIsNone(db.get_youtube_album_mapping("rg-1", "mb"))

    def test_get_returns_empty_list_after_upsert_of_empty_rows(self):
        # Resolving to an empty matrix must be visible on the next read
        # as ``[]`` (cache HIT) — not ``None`` (cache MISS).
        db = FakePipelineDB()
        db.upsert_youtube_album_mapping("rg-empty", "mb", [])
        self.assertEqual(
            db.get_youtube_album_mapping("rg-empty", "mb"), [])

    def test_upsert_inserts_new_rows_and_get_returns_them(self):
        db = FakePipelineDB()
        rows = [
            self._row(yt_browse_id="MPREb_a"),
            self._row(yt_browse_id="MPREb_b"),
        ]

        db.upsert_youtube_album_mapping("rg-1", "mb", rows)

        got = db.get_youtube_album_mapping("rg-1", "mb")
        assert got is not None
        self.assertEqual(len(got), 2)
        self.assertEqual(
            [r["yt_browse_id"] for r in got],
            ["MPREb_a", "MPREb_b"],
        )

    def test_get_returns_rows_ordered_by_yt_browse_id(self):
        """Determinism contract — order is yt_browse_id ASC regardless of insert order."""
        db = FakePipelineDB()
        rows = [
            self._row(yt_browse_id="MPREb_z"),
            self._row(yt_browse_id="MPREb_a"),
            self._row(yt_browse_id="MPREb_m"),
        ]

        db.upsert_youtube_album_mapping("rg-1", "mb", rows)

        got = db.get_youtube_album_mapping("rg-1", "mb")
        assert got is not None
        self.assertEqual(
            [r["yt_browse_id"] for r in got],
            ["MPREb_a", "MPREb_m", "MPREb_z"],
        )

    def test_upsert_atomically_replaces_existing_rows(self):
        db = FakePipelineDB()
        db.upsert_youtube_album_mapping("rg-1", "mb", [
            self._row(yt_browse_id="MPREb_old1"),
            self._row(yt_browse_id="MPREb_old2"),
            self._row(yt_browse_id="MPREb_old3"),
        ])

        # Replace with a smaller, disjoint matrix.
        db.upsert_youtube_album_mapping("rg-1", "mb", [
            self._row(yt_browse_id="MPREb_new"),
        ])

        got = db.get_youtube_album_mapping("rg-1", "mb")
        assert got is not None
        self.assertEqual(len(got), 1)
        self.assertEqual(got[0]["yt_browse_id"], "MPREb_new")

    def test_upsert_does_not_affect_other_release_group_or_source(self):
        db = FakePipelineDB()
        db.upsert_youtube_album_mapping("rg-1", "mb", [
            self._row(yt_browse_id="MPREb_a")])
        db.upsert_youtube_album_mapping("rg-2", "mb", [
            self._row(yt_browse_id="MPREb_b")])
        db.upsert_youtube_album_mapping("rg-1", "discogs", [
            self._row(yt_browse_id="MPREb_c")])

        # Replace rg-1/mb only.
        db.upsert_youtube_album_mapping("rg-1", "mb", [
            self._row(yt_browse_id="MPREb_a_v2")])

        rg1_mb = db.get_youtube_album_mapping("rg-1", "mb")
        rg2_mb = db.get_youtube_album_mapping("rg-2", "mb")
        rg1_discogs = db.get_youtube_album_mapping("rg-1", "discogs")
        assert rg1_mb is not None
        assert rg2_mb is not None
        assert rg1_discogs is not None
        self.assertEqual(
            [r["yt_browse_id"] for r in rg1_mb],
            ["MPREb_a_v2"],
        )
        self.assertEqual(
            [r["yt_browse_id"] for r in rg2_mb],
            ["MPREb_b"],
        )
        self.assertEqual(
            [r["yt_browse_id"] for r in rg1_discogs],
            ["MPREb_c"],
        )

    def test_seed_helper_populates_state(self):
        # ``seed_youtube_album_mapping`` is a fake-only backdoor that
        # bypasses ``upsert`` and stores raw stored-shape dicts directly —
        # convert the Struct via msgspec so this test still shares the
        # same fixture row as the upsert-path tests above.
        db = FakePipelineDB()
        rows = [msgspec.to_builtins(self._row(yt_browse_id="MPREb_seed"))]

        db.seed_youtube_album_mapping("rg-1", "mb", rows)

        got = db.get_youtube_album_mapping("rg-1", "mb")
        assert got is not None
        self.assertEqual(len(got), 1)
        self.assertEqual(got[0]["yt_browse_id"], "MPREb_seed")

    def test_upsert_preserves_optional_none_fields(self):
        """yt_audio_playlist_id + yt_year are NULLable per migration 034."""
        db = FakePipelineDB()
        db.upsert_youtube_album_mapping("rg-1", "mb", [
            self._row(
                yt_browse_id="MPREb_nulls",
                yt_audio_playlist_id=None,
                yt_year=None,
            ),
        ])

        got = db.get_youtube_album_mapping("rg-1", "mb")
        assert got is not None
        self.assertEqual(len(got), 1)
        self.assertIsNone(got[0]["yt_audio_playlist_id"])
        self.assertIsNone(got[0]["yt_year"])

    def test_find_mapping_for_release_matches_exact_distance(self):
        db = FakePipelineDB()
        db.upsert_youtube_album_mapping("discogs-master-1", "discogs", [
            self._row(
                yt_browse_id="MPREb_discogs",
                distances=[
                    PersistedDistance(mbid="12345", distance=0.05),
                    PersistedDistance(mbid="67890", distance=0.25),
                ],
            )
        ])

        got = db.find_youtube_album_mapping_for_release(
            source="discogs",
            release_id="12345",
            browse_id="MPREb_discogs",
        )

        self.assertIsNotNone(got)
        assert got is not None
        self.assertEqual(got["release_group_identifier"], "discogs-master-1")
        self.assertEqual(got["source"], "discogs")
        self.assertIsNone(db.find_youtube_album_mapping_for_release(
            source="mb", release_id="12345", browse_id="MPREb_discogs"))
        self.assertIsNone(db.find_youtube_album_mapping_for_release(
            source="discogs", release_id="99999", browse_id="MPREb_discogs"))
        self.assertIsNone(db.find_youtube_album_mapping_for_release(
            source="discogs", release_id="12345", browse_id="MPREb_other"))


class TestFakePipelineDBYoutubeIngest(unittest.TestCase):
    """Self-tests for FakePipelineDB YT-rescue ingest methods (U2).

    Mirror the production contract exactly:
    - ``insert_youtube_running`` raises ``YoutubeInFlightError`` on the
      second in-flight submission for the same request_id
    - ``update_youtube_terminal`` merges metadata (PG ``||`` operator)
    - ``claim_next_youtube_pending`` is FIFO by ``created_at, id``,
      excludes slskd rows and terminal rows, and stamps worker metadata
    - ``find_orphan_youtube_running`` returns claimed in-flight ids only
    """

    def _payload(self, request_id: int, **overrides: Any) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "request_id": request_id,
            "browse_id": "MPREb_default",
            "audio_playlist_id": "OLAK5uy_default",
            "yt_url": "https://music.youtube.com/playlist?list=OLAK5uy_default",
            "expected_track_count": 10,
        }
        payload.update(overrides)
        return payload

    def test_insert_youtube_running_writes_row_with_metadata(self):
        db = FakePipelineDB()
        log_id = db.insert_youtube_running(**self._payload(42))
        self.assertEqual(len(db.download_logs), 1)
        row = db.download_logs[0]
        self.assertEqual(row.id, log_id)
        self.assertEqual(row.request_id, 42)
        self.assertEqual(row.source, "youtube")
        self.assertEqual(row.outcome, "youtube_running")
        assert row.youtube_metadata is not None
        self.assertEqual(row.youtube_metadata["browse_id"], "MPREb_default")
        self.assertEqual(row.youtube_metadata["expected_track_count"], 10)

    def test_insert_youtube_running_raises_youtube_in_flight_error(self):
        from lib.pipeline_db import YoutubeInFlightError
        db = FakePipelineDB()
        first_id = db.insert_youtube_running(**self._payload(42))
        with self.assertRaises(YoutubeInFlightError) as ctx:
            db.insert_youtube_running(**self._payload(
                42, browse_id="MPREb_collide",
            ))
        self.assertEqual(ctx.exception.existing_download_log_id, first_id)
        self.assertEqual(ctx.exception.request_id, 42)

    def test_insert_after_terminal_succeeds(self):
        db = FakePipelineDB()
        first_id = db.insert_youtube_running(**self._payload(42))
        db.update_youtube_terminal(
            first_id, "youtube_failed", {"reason": "test"},
        )
        # The fake mirrors the partial-unique-index contract: terminal
        # rows do NOT block re-submission.
        second_id = db.insert_youtube_running(**self._payload(
            42, browse_id="MPREb_after_terminal",
        ))
        self.assertNotEqual(first_id, second_id)

    def test_update_youtube_terminal_merges_metadata(self):
        db = FakePipelineDB()
        log_id = db.insert_youtube_running(**self._payload(42))
        db.update_youtube_terminal(log_id, "youtube_success", {
            "observed_track_count": 10,
            "per_track_video_ids": ["v1", "v2"],
        })
        entry = db.get_download_log_entry(log_id)
        assert entry is not None
        self.assertEqual(entry["outcome"], "youtube_success")
        meta = cast(dict, entry["youtube_metadata"])
        self.assertIsInstance(meta, dict)
        # Submission-time fields survive.
        self.assertEqual(meta["browse_id"], "MPREb_default")
        # Terminal fields are layered on top.
        self.assertEqual(meta["observed_track_count"], 10)
        self.assertEqual(meta["per_track_video_ids"], ["v1", "v2"])

    def test_update_youtube_terminal_rejects_non_terminal_outcomes(self):
        db = FakePipelineDB()
        log_id = db.insert_youtube_running(**self._payload(42))
        for bogus in ("youtube_running", "success", "rejected", ""):
            with self.subTest(outcome=bogus), self.assertRaises(ValueError):
                db.update_youtube_terminal(log_id, bogus, {})

    def test_claim_next_youtube_pending_filters_by_source_and_outcome(self):
        db = FakePipelineDB()
        # An slskd-side row.
        db.log_download(
            42, soulseek_username="alice", outcome="success",
        )
        # An in-flight YT row.
        yt_id = db.insert_youtube_running(**self._payload(42))
        rows = db.claim_next_youtube_pending(worker_id="w", limit=10)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["id"], yt_id)
        self.assertEqual(rows[0]["source"], "youtube")

    def test_claim_next_youtube_pending_excludes_terminal_rows(self):
        db = FakePipelineDB()
        log_id = db.insert_youtube_running(**self._payload(42))
        # A terminal (never-claimed) row is not drainable.
        db.update_youtube_terminal(log_id, "youtube_success", {})
        self.assertEqual(db.claim_next_youtube_pending(worker_id="w", limit=10), [])

    def test_claim_next_youtube_pending_is_fifo(self):
        db = FakePipelineDB()
        first = db.insert_youtube_running(**self._payload(42))
        second = db.insert_youtube_running(**self._payload(
            43, browse_id="MPREb_43",
        ))
        rows = db.claim_next_youtube_pending(worker_id="w", limit=10)
        self.assertEqual([r["id"] for r in rows], [first, second])

    def test_claim_next_youtube_pending_marks_worker_metadata(self):
        db = FakePipelineDB()
        first = db.insert_youtube_running(**self._payload(42))
        second = db.insert_youtube_running(**self._payload(
            43, browse_id="MPREb_43",
        ))
        claimed = db.claim_next_youtube_pending(worker_id="worker-1", limit=1)
        self.assertEqual([r["id"] for r in claimed], [first])
        # The unclaimed sibling is still drainable by the next claim.
        self.assertEqual(
            [r["id"] for r in db.claim_next_youtube_pending(
                worker_id="worker-2", limit=10)],
            [second],
        )
        meta = claimed[0]["youtube_metadata"]
        self.assertEqual(meta["worker_id"], "worker-1")
        self.assertIsNotNone(meta["worker_claimed_at"])

    def test_find_orphan_youtube_running_returns_claimed_ids(self):
        db = FakePipelineDB()
        first = db.insert_youtube_running(**self._payload(42))
        second = db.insert_youtube_running(**self._payload(
            43, browse_id="MPREb_43",
        ))
        self.assertEqual(db.find_orphan_youtube_running(), [])
        db.claim_next_youtube_pending(worker_id="worker-1", limit=1)
        orphans = db.find_orphan_youtube_running()
        self.assertEqual(orphans, [first])
        for log_id in orphans:
            db.update_youtube_terminal(
                log_id, "youtube_failed", {"reason": "worker_interrupted"},
            )
        self.assertEqual(db.find_orphan_youtube_running(), [])
        # The surviving sibling is still drainable after the orphan sweep.
        self.assertEqual(
            [r["id"] for r in db.claim_next_youtube_pending(
                worker_id="worker-2", limit=10)],
            [second],
        )

    def test_read_seam_includes_source_and_youtube_metadata(self):
        db = FakePipelineDB()
        slskd_id = db.log_download(
            42, soulseek_username="alice", outcome="success",
        )
        yt_id = db.insert_youtube_running(**self._payload(42))

        slskd_entry = db.get_download_log_entry(slskd_id)
        assert slskd_entry is not None
        self.assertEqual(slskd_entry["source"], "slskd")
        self.assertIsNone(slskd_entry["youtube_metadata"])

        yt_entry = db.get_download_log_entry(yt_id)
        assert yt_entry is not None
        self.assertEqual(yt_entry["source"], "youtube")
        self.assertIsInstance(yt_entry["youtube_metadata"], dict)

        # get_download_history surfaces both rows.
        history = db.get_download_history(42)
        sources = {r["source"] for r in history}
        self.assertEqual(sources, {"slskd", "youtube"})

        # get_download_history_batch likewise.
        batch = db.get_download_history_batch([42])
        self.assertEqual(
            {r["source"] for r in batch[42]}, {"slskd", "youtube"},
        )


if __name__ == "__main__":
    unittest.main()
