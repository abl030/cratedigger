"""Tests for lightweight fakes and shared builders."""

import inspect
import unittest
from datetime import datetime, timedelta, timezone
from typing import Any

from lib.grab_list import DownloadFile, GrabListEntry
from lib.pipeline_db import PipelineDB, RequestSpectralStateUpdate
from lib.quality import SpectralContext, SpectralMeasurement, ValidationResult
from tests.fakes import FakePipelineDB, FakeSlskdAPI
from tests.helpers import (
    make_download_file,
    make_grab_list_entry,
    make_request_row,
    make_spectral_context,
    make_validation_result,
)


class TestFakePipelineDB(unittest.TestCase):
    def test_record_attempt_updates_retry_metadata(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="wanted"))

        db.record_attempt(42, "validation")

        row = db.request(42)
        self.assertEqual(row["validation_attempts"], 1)
        self.assertIsNotNone(row["last_attempt_at"])
        self.assertIsNotNone(row["next_retry_after"])
        self.assertIsNotNone(row["updated_at"])
        self.assertEqual(db.recorded_attempts, [(42, "validation")])

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

    def test_update_download_state_rewrites_json_state(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="downloading"))

        db.update_download_state(42, '{"filetype":"flac"}')

        row = db.request(42)
        self.assertEqual(row["status"], "downloading")
        self.assertEqual(row["active_download_state"], {"filetype": "flac"})
        self.assertEqual(
            db.update_download_state_calls,
            [(42, '{"filetype":"flac"}')],
        )

    def test_update_spectral_state(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42))

        update = RequestSpectralStateUpdate(
            current=SpectralMeasurement(grade="genuine", bitrate_kbps=None),
        )
        db.update_spectral_state(42, update)

        row = db.request(42)
        self.assertEqual(row["current_spectral_grade"], "genuine")
        self.assertIsNone(row["current_spectral_bitrate"])

    def test_update_imported_path_by_release_id_matches_mb_albumid(self):
        """Issue #132 P2 / #133: sibling ``imported_path`` propagation.
        MB-sourced match on ``mb_release_id``."""
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=17, mb_release_id="mbid-sibling",
            imported_path="/Beets/Old/Path"))

        rows = db.update_imported_path_by_release_id(
            mb_albumid="mbid-sibling",
            discogs_albumid="",
            new_path="/Beets/New/Path [2006]",
        )

        self.assertEqual(rows, 1)
        self.assertEqual(
            db.request(17)["imported_path"], "/Beets/New/Path [2006]")

    def test_update_imported_path_by_release_id_matches_discogs(self):
        """Discogs-sourced match on ``discogs_release_id``."""
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=18, mb_release_id=None,
            discogs_release_id="12856590",
            imported_path="/Beets/Old/Discogs"))

        rows = db.update_imported_path_by_release_id(
            mb_albumid="",
            discogs_albumid="12856590",
            new_path="/Beets/New/Discogs [2006]",
        )

        self.assertEqual(rows, 1)
        self.assertEqual(
            db.request(18)["imported_path"], "/Beets/New/Discogs [2006]")

    def test_update_imported_path_by_release_id_untracked_returns_zero(self):
        """No matching request → rowcount=0, no rows touched."""
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=19, mb_release_id="other-mbid",
            imported_path="/Beets/Other"))

        rows = db.update_imported_path_by_release_id(
            mb_albumid="unknown-mbid",
            discogs_albumid="",
            new_path="/Beets/Ignored",
        )

        self.assertEqual(rows, 0)
        self.assertEqual(db.request(19)["imported_path"], "/Beets/Other")

    def test_update_imported_path_discogs_matches_legacy_mb_release_id(self):
        """Codex R2 P2: beets-side ``discogs_albumid`` must match
        pipeline rows that stored the Discogs numeric in
        ``mb_release_id`` (legacy "pipeline compat" layout from
        CLAUDE.md) OR in ``discogs_release_id``."""
        db = FakePipelineDB()
        # Legacy layout: numeric in mb_release_id, discogs_release_id None.
        db.seed_request(make_request_row(
            id=21, mb_release_id="12856590",
            discogs_release_id=None, imported_path="/Beets/Legacy/Old"))

        rows = db.update_imported_path_by_release_id(
            mb_albumid="",
            discogs_albumid="12856590",
            new_path="/Beets/Legacy/New",
        )

        self.assertEqual(rows, 1)
        self.assertEqual(
            db.request(21)["imported_path"], "/Beets/Legacy/New")

    def test_update_imported_path_by_release_id_both_empty_is_noop(self):
        """Both release ids empty → rowcount=0, no UPDATE fires at all.
        Mirrors the prod short-circuit that guards against accidentally
        matching every row where a column is NULL/empty."""
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=20, mb_release_id="some-mbid",
            imported_path="/Beets/Keep"))

        rows = db.update_imported_path_by_release_id(
            mb_albumid="", discogs_albumid="", new_path="/Beets/Bogus")

        self.assertEqual(rows, 0)
        self.assertEqual(db.request(20)["imported_path"], "/Beets/Keep")

    def test_clear_on_disk_quality_fields_matches_real_db(self):
        """FakePipelineDB must mirror PipelineDB.clear_on_disk_quality_fields:
        zero the on-disk spectral + verified_lossless + imported_path,
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
            imported_path="/mnt/virtio/Music/Beets/Stale/Path",
        ))

        db.clear_on_disk_quality_fields(42)

        row = db.request(42)
        self.assertFalse(row["verified_lossless"])
        self.assertIsNone(row["current_spectral_grade"])
        self.assertIsNone(row["current_spectral_bitrate"])
        self.assertIsNone(row["imported_path"],
                          "imported_path must clear — the web UI renders it "
                          "directly and a stale path after beet rm is worse "
                          "than no path at all.")
        # min_bitrate preserved as baseline for next gate.
        self.assertEqual(row["min_bitrate"], 320)
        # Recent download's spectral is an audit trail, not on-disk state.
        self.assertEqual(row["last_download_spectral_grade"], "suspect")
        self.assertEqual(row["last_download_spectral_bitrate"], 192)

    def test_get_downloading(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="downloading"))
        db.seed_request(make_request_row(id=2, status="wanted"))
        db.seed_request(make_request_row(id=3, status="downloading"))

        rows = db.get_downloading()
        self.assertEqual(len(rows), 2)
        ids = {r["id"] for r in rows}
        self.assertEqual(ids, {1, 3})

    def test_assert_log_passes(self):
        db = FakePipelineDB()
        db.log_download(42, outcome="success", soulseek_username="user1")

        # Should not raise
        db.assert_log(self, 0, outcome="success", request_id=42)

    def test_assert_log_checks_extra_fields(self):
        db = FakePipelineDB()
        db.log_download(42, outcome="success", spectral_grade="genuine")

        db.assert_log(self, 0, outcome="success")
        # Extra field goes into .extra dict
        self.assertEqual(db.download_logs[0].extra["spectral_grade"], "genuine")

    def test_advisory_lock_default_yields_true(self):
        db = FakePipelineDB()
        with db.advisory_lock(0x1234, 42) as acquired:
            self.assertTrue(acquired)
        self.assertEqual(db.advisory_lock_calls, [(0x1234, 42)])

    def test_advisory_lock_configurable(self):
        db = FakePipelineDB()
        db.set_advisory_lock_result(False)
        with db.advisory_lock(0x1234, 42) as acquired:
            self.assertFalse(acquired)
        self.assertEqual(db.advisory_lock_calls, [(0x1234, 42)])


class TestFakeSlskdAPI(unittest.TestCase):
    def test_get_downloads_returns_queued_snapshots(self):
        first = [{"username": "user1", "directories": [{"files": []}]}]
        second = [{"username": "user1", "directories": [{"files": [
            {"filename": "track.mp3", "id": "tid-1"},
        ]}]}]
        slskd = FakeSlskdAPI(download_snapshots=[first, second])

        self.assertEqual(slskd.transfers.get_all_downloads(includeRemoved=True), first)
        self.assertEqual(slskd.transfers.get_all_downloads(includeRemoved=True), second)
        self.assertEqual(slskd.transfers.get_all_downloads(includeRemoved=True), second)
        self.assertEqual(slskd.transfers.get_all_downloads_calls, [True, True, True])

    def test_get_download_matches_username_and_id(self):
        slskd = FakeSlskdAPI()
        slskd.add_transfer(
            username="user1",
            directory="user1\\Music",
            filename="user1\\Music\\01.flac",
            id="tid-1",
            state="Completed, Succeeded",
        )

        transfer = slskd.transfers.get_download("user1", "tid-1")

        self.assertEqual(transfer["filename"], "user1\\Music\\01.flac")
        self.assertEqual(transfer["state"], "Completed, Succeeded")
        self.assertEqual(slskd.transfers.get_download_calls, [("user1", "tid-1")])

    def test_records_enqueue_and_cancel_calls(self):
        slskd = FakeSlskdAPI()
        files = [{"filename": "track.mp3", "size": 1000}]

        self.assertTrue(slskd.transfers.enqueue("user1", files))
        self.assertTrue(slskd.transfers.cancel_download("user1", "tid-1"))

        self.assertEqual(slskd.transfers.enqueue_calls[0].username, "user1")
        self.assertEqual(slskd.transfers.enqueue_calls[0].files, files)
        self.assertEqual(slskd.transfers.cancel_download_calls[0].id, "tid-1")

    def test_user_directories_record_results_and_errors(self):
        slskd = FakeSlskdAPI()
        directory = [{"directory": "Music\\Album", "files": []}]
        slskd.users.set_directory("user1", "Music\\Album", directory)
        slskd.users.set_directory_error(
            "user1",
            "Music\\Broken",
            Exception("Peer offline"),
        )

        self.assertEqual(slskd.users.directory("user1", "Music\\Album"), directory)
        with self.assertRaises(Exception):
            slskd.users.directory("user1", "Music\\Broken")
        self.assertEqual(slskd.users.directory_calls, [
            ("user1", "Music\\Album"),
            ("user1", "Music\\Broken"),
        ])


class TestBuilders(unittest.TestCase):
    def test_make_download_file_defaults(self):
        f = make_download_file()
        self.assertIsInstance(f, DownloadFile)
        self.assertEqual(f.filename, "01 - Track.mp3")
        self.assertEqual(f.username, "user1")
        self.assertEqual(f.size, 5_000_000)

    def test_make_download_file_overrides(self):
        f = make_download_file(username="beta", bitRate=192)
        self.assertEqual(f.username, "beta")
        self.assertEqual(f.bitRate, 192)

    def test_make_grab_list_entry_defaults(self):
        entry = make_grab_list_entry()
        self.assertIsInstance(entry, GrabListEntry)
        self.assertEqual(entry.artist, "Test Artist")
        self.assertEqual(len(entry.files), 1)
        self.assertIsInstance(entry.files[0], DownloadFile)

    def test_make_grab_list_entry_overrides(self):
        files = [make_download_file(username="a"), make_download_file(username="b")]
        entry = make_grab_list_entry(files=files, db_request_id=42, db_source="request")
        self.assertEqual(len(entry.files), 2)
        self.assertEqual(entry.db_request_id, 42)

    def test_make_validation_result_defaults(self):
        vr = make_validation_result()
        self.assertIsInstance(vr, ValidationResult)
        self.assertTrue(vr.valid)
        self.assertEqual(vr.distance, 0.05)
        self.assertEqual(vr.scenario, "strong_match")

    def test_make_validation_result_overrides(self):
        vr = make_validation_result(valid=False, distance=0.5, scenario="bad_match",
                                     failed_path="/tmp/failed")
        self.assertFalse(vr.valid)
        self.assertEqual(vr.distance, 0.5)
        self.assertEqual(vr.failed_path, "/tmp/failed")

    def test_make_spectral_context_defaults(self):
        sc = make_spectral_context()
        self.assertIsInstance(sc, SpectralContext)
        self.assertFalse(sc.needs_check)
        self.assertIsNone(sc.grade)

    def test_make_spectral_context_overrides(self):
        sc = make_spectral_context(needs_check=True, grade="suspect", bitrate=192)
        self.assertTrue(sc.needs_check)
        self.assertEqual(sc.grade, "suspect")
        self.assertEqual(sc.bitrate, 192)


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


class TestFakePipelineDBNewStubs(unittest.TestCase):
    """Self-tests for fake methods retroactively added under issue #140.

    These cover behaviour that tests relying on the fake may start
    exercising. Matches the rule in ``.claude/rules/code-quality.md``:
    "every new PipelineDB method needs an equivalent stub on
    FakePipelineDB with a self-test in tests/test_fakes.py."
    """

    def test_close_marks_flag(self):
        db = FakePipelineDB()
        self.assertFalse(db.closed)
        db.close()
        self.assertTrue(db.closed)

    def test_add_request_assigns_monotonic_id(self):
        db = FakePipelineDB()
        rid1 = db.add_request("Artist A", "Album A", source="request")
        rid2 = db.add_request("Artist B", "Album B", source="request")
        self.assertEqual((rid1, rid2), (1, 2))
        self.assertEqual(db.request(rid1)["artist_name"], "Artist A")
        self.assertEqual(db.request(rid2)["status"], "wanted")

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
        # Populate download history for both then ensure ``get_recent``
        # also sorts through the mixed shapes without raising.
        db.log_download(1, outcome="success")
        db.log_download(2, outcome="success")
        recent = db.get_recent()
        self.assertEqual({r["id"] for r in recent}, {1, 2})

    def test_delete_request_removes_row_and_tracks(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1))
        db.set_tracks(1, [{"track_number": 1, "title": "T"}])
        db.delete_request(1)
        self.assertNotIn(1, db._requests)  # type: ignore[attr-defined]
        self.assertEqual(db.get_tracks(1), [])

    def test_delete_request_cascades_to_child_tables(self):
        """Real SQL has ``ON DELETE CASCADE`` from album_requests to
        download_log, search_log, and source_denylist. The fake must
        prune those too so tests cannot observe an impossible state
        where orphaned child rows survive their parent (codex R2)."""
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1))
        db.seed_request(make_request_row(id=2))
        db.log_download(1, outcome="success")
        db.log_download(2, outcome="success")
        db.log_search(1, outcome="found")
        db.log_search(2, outcome="no_match")
        db.add_denylist(1, "badguy")
        db.add_denylist(2, "other")

        db.delete_request(1)

        self.assertEqual([e.request_id for e in db.download_logs], [2])
        self.assertEqual([e.request_id for e in db.search_logs], [2])
        self.assertEqual([e.request_id for e in db.denylist], [2])

    def test_get_wanted_prioritizes_new_and_respects_limit(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="wanted",
                                          search_attempts=0))
        db.seed_request(make_request_row(id=2, status="wanted",
                                          search_attempts=5))
        db.seed_request(make_request_row(id=3, status="imported"))
        rows = db.get_wanted()
        self.assertEqual([r["id"] for r in rows], [1, 2])
        self.assertEqual(
            [r["id"] for r in db.get_wanted(limit=1)], [1])

    def test_get_wanted_skips_albums_inside_retry_window(self):
        db = FakePipelineDB()
        future = datetime.now(timezone.utc) + timedelta(hours=1)
        db.seed_request(make_request_row(
            id=1, status="wanted", next_retry_after=future))
        db.seed_request(make_request_row(id=2, status="wanted"))
        rows = db.get_wanted()
        self.assertEqual([r["id"] for r in rows], [2])

    def test_get_wanted_tie_break_is_set_not_order(self):
        """Within a priority bucket the real DB randomises order —
        callers must assert on set membership, not list position."""
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=1, status="wanted", search_attempts=0))
        db.seed_request(make_request_row(
            id=2, status="wanted", search_attempts=0))
        db.seed_request(make_request_row(
            id=3, status="wanted", search_attempts=0))
        rows = db.get_wanted()
        self.assertEqual({r["id"] for r in rows}, {1, 2, 3})

    def test_get_log_filters_and_orders_newest_first(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, album_title="Album A"))
        db.log_download(1, outcome="success")
        db.log_download(1, outcome="failed")
        db.log_download(1, outcome="rejected")
        all_rows = db.get_log()
        self.assertEqual([r["outcome"] for r in all_rows],
                         ["rejected", "failed", "success"])
        imported = db.get_log(outcome_filter="imported")
        self.assertEqual([r["outcome"] for r in imported], ["success"])
        rejected = db.get_log(outcome_filter="rejected")
        self.assertEqual([r["outcome"] for r in rejected],
                         ["rejected", "failed"])
        # Joined request columns present.
        self.assertEqual(all_rows[0]["album_title"], "Album A")

    def test_get_log_surfaces_auxiliary_columns(self):
        """Real ``get_log`` returns ``dl.*`` — every ``log_download``
        column must be present, including fields parked in
        ``entry.extra`` (bitrate, spectral_grade, final_format, etc.)
        Codex R2: callers that feed these rows into LogEntry.from_row
        would otherwise classify incomplete data (codex R2)."""
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1))
        db.log_download(
            1, outcome="success",
            bitrate=256, spectral_grade="genuine",
            final_format="mp3 v0", actual_min_bitrate=245)
        rows = db.get_log()
        self.assertEqual(rows[0]["bitrate"], 256)
        self.assertEqual(rows[0]["spectral_grade"], "genuine")
        self.assertEqual(rows[0]["final_format"], "mp3 v0")
        self.assertEqual(rows[0]["actual_min_bitrate"], 245)

    def test_get_by_status_sorts_by_created_at(self):
        db = FakePipelineDB()
        now = datetime.now(timezone.utc)
        db.seed_request(make_request_row(
            id=1, status="wanted", created_at=now + timedelta(seconds=2)))
        db.seed_request(make_request_row(
            id=2, status="wanted", created_at=now))
        rows = db.get_by_status("wanted")
        self.assertEqual([r["id"] for r in rows], [2, 1])

    def test_get_recent_requires_download_history(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1))
        db.seed_request(make_request_row(id=2))
        db.log_download(1, outcome="success")
        rows = db.get_recent()
        self.assertEqual([r["id"] for r in rows], [1])

    def test_get_recent_deterministic_with_missing_updated_at(self):
        """Sort key must not call ``_utcnow()`` per comparison —
        multiple rows with no ``updated_at`` must fall into a stable
        insertion order so tests cannot flake."""
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, updated_at=None))
        db.seed_request(make_request_row(id=2, updated_at=None))
        db.seed_request(make_request_row(id=3, updated_at=None))
        db.log_download(1, outcome="success")
        db.log_download(2, outcome="success")
        db.log_download(3, outcome="success")
        rows = db.get_recent()
        self.assertEqual({r["id"] for r in rows}, {1, 2, 3})

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

    def test_tracks_round_trip_and_count(self):
        db = FakePipelineDB()
        db.set_tracks(1, [
            {"track_number": 2, "title": "Second"},
            {"track_number": 1, "title": "First"},
        ])
        rows = db.get_tracks(1)
        self.assertEqual([t["track_number"] for t in rows], [1, 2])
        self.assertEqual(db.get_track_counts([1, 99]), {1: 2})

    def test_download_log_history_and_lookup_by_id(self):
        db = FakePipelineDB()
        db.log_download(1, outcome="success")
        db.log_download(1, outcome="failed")
        db.log_download(2, outcome="rejected")

        history_1 = db.get_download_history(1)
        self.assertEqual([r["outcome"] for r in history_1],
                         ["failed", "success"])
        batch = db.get_download_history_batch([1, 2])
        self.assertEqual({k: [r["outcome"] for r in v]
                          for k, v in batch.items()},
                         {1: ["failed", "success"], 2: ["rejected"]})

        first_id = db.download_logs[0].id
        entry = db.get_download_log_entry(first_id)
        assert entry is not None
        self.assertEqual(entry["outcome"], "success")
        self.assertIsNone(db.get_download_log_entry(99999))

    def test_get_wrong_matches_collapses_per_request_and_path(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=1, artist_name="A", album_title="B"))
        # Two rejections on the same (request, failed_path) — keep newest.
        db.log_download(1, outcome="rejected",
                        validation_result={"failed_path": "/p1"})
        db.log_download(1, outcome="rejected",
                        validation_result={"failed_path": "/p1"})
        # Different path — separate row.
        db.log_download(1, outcome="rejected",
                        validation_result={"failed_path": "/p2"})
        # Scenario filtered out.
        db.log_download(1, outcome="rejected", validation_result={
            "failed_path": "/p3", "scenario": "audio_corrupt"})
        # Non-rejected — ignored.
        db.log_download(1, outcome="success",
                        validation_result={"failed_path": "/p4"})

        rows = db.get_wrong_matches()
        paths = sorted([
            (r["validation_result"] or {}).get("failed_path")  # type: ignore[union-attr]
            for r in rows])
        self.assertEqual(paths, ["/p1", "/p2"])

    def test_clear_wrong_match_path_strips_key(self):
        db = FakePipelineDB()
        db.log_download(1, outcome="rejected",
                        validation_result={"failed_path": "/p1",
                                           "scenario": "wrong_match"})
        log_id = db.download_logs[0].id
        self.assertTrue(db.clear_wrong_match_path(log_id))
        vr = db.download_logs[0].validation_result
        assert isinstance(vr, dict)
        self.assertNotIn("failed_path", vr)
        self.assertEqual(vr["scenario"], "wrong_match")
        # Second call returns False (already stripped).
        self.assertFalse(db.clear_wrong_match_path(log_id))

    def test_clear_wrong_match_path_handles_json_string(self):
        """Real ``validation_result`` is JSONB — fakes also accept JSON
        strings so tests can pass either shape."""
        import json as _json
        db = FakePipelineDB()
        db.log_download(1, outcome="rejected",
                        validation_result=_json.dumps(
                            {"failed_path": "/p", "x": 1}))
        self.assertTrue(
            db.clear_wrong_match_path(db.download_logs[0].id))
        stored = _json.loads(db.download_logs[0].validation_result)  # type: ignore[arg-type]
        self.assertNotIn("failed_path", stored)

    def test_search_log_history_and_batch(self):
        db = FakePipelineDB()
        db.log_search(1, query="a b", outcome="found", result_count=10,
                      elapsed_s=0.5)
        db.log_search(1, query="c d", outcome="no_match")
        db.log_search(2, query="e f", outcome="error")

        history_1 = db.get_search_history(1)
        self.assertEqual([r["outcome"] for r in history_1],
                         ["no_match", "found"])
        batch = db.get_search_history_batch([1, 2])
        self.assertEqual(
            {k: [r["outcome"] for r in v] for k, v in batch.items()},
            {1: ["no_match", "found"], 2: ["error"]})

    def test_user_cooldowns_upsert_and_filter(self):
        db = FakePipelineDB()
        now = datetime.now(timezone.utc)
        db.add_cooldown("alice", now + timedelta(days=3), reason="x")
        db.add_cooldown("bob", now - timedelta(days=1), reason="expired")
        # Upsert — second call on alice replaces cooldown_until/reason.
        db.add_cooldown("alice", now + timedelta(days=7), reason="y")

        active = db.get_cooled_down_users()
        self.assertEqual(active, ["alice"])

        rows = db.get_user_cooldowns()
        # Newest cooldown_until first.
        self.assertEqual([r["username"] for r in rows], ["alice", "bob"])
        self.assertEqual(rows[0]["reason"], "y")


def _public_methods(cls: type) -> set[str]:
    """Return the set of non-underscore method names defined on ``cls``."""
    return {
        name for name, obj in vars(cls).items()
        if callable(obj) and not name.startswith("_")
    }


class TestPipelineDBFakeContract(unittest.TestCase):
    """Enforce FakePipelineDB stays in lockstep with PipelineDB.

    Models ``TestRouteContractAudit`` (tests/test_web_server.py): the
    convention in ``.claude/rules/code-quality.md`` — "every new
    PipelineDB method must have a matching stub on FakePipelineDB with
    a self-test in tests/test_fakes.py" — is enforced at test time, not
    at review time.

    Silent drift was possible before this test existed. In PR #136
    ``update_imported_path_by_release_id`` only got its direct self-test
    after the final-review agent flagged it; any orchestration test
    that tried to call the method via a fake that lacked it would have
    crashed with ``AttributeError``. A new kwarg on a real method would
    be silently swallowed if the fake accepted ``**kwargs``.
    """

    def test_fake_exposes_every_public_method_of_real(self) -> None:
        """Every non-underscore method on ``PipelineDB`` must exist on
        ``FakePipelineDB``."""
        real = _public_methods(PipelineDB)
        fake = _public_methods(FakePipelineDB)
        missing = real - fake
        self.assertEqual(
            missing, set(),
            f"FakePipelineDB is missing stubs for: {sorted(missing)}. "
            "See .claude/rules/code-quality.md 'New PipelineDB method' "
            "in the new-work checklist.",
        )

    def test_fake_signatures_compatible_with_real(self) -> None:
        """For every shared method, each named parameter on the real
        method must be declared by name on the fake with a compatible
        kind and no stricter requiredness.

        This catches "real added a new kwarg; fake silently ignored it"
        drift. Crucially, a bare ``**kwargs`` on the fake is NOT allowed
        to absorb a named real parameter — otherwise a fake that
        accepts ``**kwargs`` would pass this check for any real
        signature, reproducing the exact silent-drift failure mode the
        contract is meant to prevent.

        ``**kwargs`` on the fake may still absorb test-only extras and
        matches the real's own ``**kwargs`` when present. Return types
        and type annotations are not checked — the fake is free to use
        ``Any`` for brevity.
        """
        mismatches = _diff_signatures(PipelineDB, FakePipelineDB)
        self.assertEqual(
            mismatches, [],
            "FakePipelineDB signatures drifted from PipelineDB. "
            "Every real parameter must be named explicitly on the fake "
            "(bare **kwargs does NOT satisfy the contract). "
            "Mismatches:\n  "
            + "\n  ".join(mismatches),
        )


def _diff_signatures(real_cls: type, fake_cls: type) -> list[str]:
    """Return a list of signature drift messages between two classes.

    Extracted so ``TestPipelineDBFakeContractInternals`` can directly
    exercise the drift detector against synthetic classes without
    mutating the real fake.
    """
    real_methods = _public_methods(real_cls)
    fake_methods = _public_methods(fake_cls)
    shared = real_methods & fake_methods

    mismatches: list[str] = []
    for name in sorted(shared):
        real_sig = inspect.signature(getattr(real_cls, name))
        fake_sig = inspect.signature(getattr(fake_cls, name))

        fake_params = fake_sig.parameters
        fake_accepts_varargs = any(
            p.kind == inspect.Parameter.VAR_POSITIONAL
            for p in fake_params.values()
        )
        fake_accepts_kwargs = any(
            p.kind == inspect.Parameter.VAR_KEYWORD
            for p in fake_params.values()
        )

        for pname, param in real_sig.parameters.items():
            if pname == "self":
                continue
            if param.kind == inspect.Parameter.VAR_POSITIONAL:
                if not fake_accepts_varargs:
                    mismatches.append(
                        f"{name}: real has *{pname} but fake does "
                        "not accept variable positional args")
                continue
            if param.kind == inspect.Parameter.VAR_KEYWORD:
                if not fake_accepts_kwargs:
                    mismatches.append(
                        f"{name}: real has **{pname} but fake does "
                        "not accept variable keyword args")
                continue
            # Regular named parameter (positional-or-keyword or
            # keyword-only). MUST be declared on the fake —
            # **kwargs absorption is not sufficient.
            if pname not in fake_params:
                mismatches.append(
                    f"{name}: param '{pname}' present on real but "
                    "not declared on fake (declare it explicitly — "
                    "**kwargs does not count)")
                continue
            fp = fake_params[pname]
            # Allowed kind transitions: same kind, or
            # real=positional-or-keyword → fake=keyword-only
            # (fake is stricter but still callable via keyword).
            allowed = (
                fp.kind == param.kind
                or (param.kind
                    == inspect.Parameter.POSITIONAL_OR_KEYWORD
                    and fp.kind
                    == inspect.Parameter.KEYWORD_ONLY)
            )
            if not allowed:
                mismatches.append(
                    f"{name}({pname}): kind mismatch — "
                    f"real={param.kind.name}, "
                    f"fake={fp.kind.name}")
                continue
            # Requiredness: a real param without default must have
            # no default on the fake either — otherwise the fake
            # silently makes the arg optional.
            real_required = param.default is inspect.Parameter.empty
            fake_required = fp.default is inspect.Parameter.empty
            if real_required and not fake_required:
                mismatches.append(
                    f"{name}({pname}): real requires this param but "
                    "fake gives it a default (silently makes it "
                    "optional)")
    return mismatches


class TestPipelineDBFakeContractInternals(unittest.TestCase):
    """Regression tests for the drift detector itself.

    The detector must fail when real and fake disagree, otherwise the
    outer contract test is a silent no-op. Exercise the drift cases
    directly.
    """

    def test_kwargs_does_not_absorb_named_param(self):
        """Bare **kwargs on fake must NOT satisfy a named real param."""
        class Real:
            def m(self, request_id: int, flag: bool = False) -> None:
                ...
        class Fake:
            def m(self, request_id: int, **kwargs: Any) -> None:
                ...
        diff = _diff_signatures(Real, Fake)
        self.assertTrue(
            any("'flag'" in m for m in diff),
            f"Expected drift for named param 'flag', got: {diff}")

    def test_renamed_param_is_caught(self):
        class Real:
            def m(self, spectral_grade: str | None = None) -> None:
                ...
        class Fake:
            def m(self, grade: str | None = None) -> None:
                ...
        diff = _diff_signatures(Real, Fake)
        self.assertTrue(
            any("'spectral_grade'" in m for m in diff),
            f"Expected drift for renamed param, got: {diff}")

    def test_required_becoming_optional_is_caught(self):
        class Real:
            def m(self, release_id: str) -> None:
                ...
        class Fake:
            def m(self, release_id: str = "") -> None:
                ...
        diff = _diff_signatures(Real, Fake)
        self.assertTrue(
            any("release_id" in m and "optional" in m for m in diff),
            f"Expected requiredness drift, got: {diff}")

    def test_clean_signature_yields_no_diff(self):
        class Real:
            def m(self, request_id: int, flag: bool = False) -> None:
                ...
        class Fake:
            def m(self, request_id: int, flag: bool = False) -> None:
                ...
        self.assertEqual(_diff_signatures(Real, Fake), [])

    def test_star_kwargs_on_real_still_requires_fake_kwargs(self):
        class Real:
            def m(self, **extra: Any) -> None:
                ...
        class Fake:
            def m(self) -> None:  # no **kwargs
                ...
        diff = _diff_signatures(Real, Fake)
        self.assertTrue(
            any("**extra" in m for m in diff),
            f"Expected drift when fake drops **kwargs, got: {diff}")
