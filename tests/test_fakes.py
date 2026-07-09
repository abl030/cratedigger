"""Tests for lightweight fakes and shared builders."""

import inspect
import unittest
from datetime import date, datetime, timedelta, timezone
from typing import Any
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

import msgspec

from lib.grab_list import DownloadFile, GrabListEntry
from lib.pipeline_db import (
    PersistedDistance,
    PersistedTrack,
    PersistedYoutubeRow,
    PipelineDB,
    RequestSpectralStateUpdate,
    TransferLedgerRow,
)
from lib.quality import SpectralMeasurement, ValidationResult
from tests.fakes import (
    FakeBeetsDB,
    FakeCursor,
    FakePipelineDB,
    FakeSlskdAPI,
    FakeYTMusic,
)
from tests.helpers import (
    make_album_quality_evidence,
    make_download_file,
    make_grab_list_entry,
    make_request_row,
    make_validation_result,
)


class TestFakePipelineDB(unittest.TestCase):
    def test_album_quality_evidence_round_trips_by_content_key(self):
        from lib.quality import AlbumQualityEvidenceFile

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42))
        evidence = make_album_quality_evidence(
            mb_release_id="mb-roundtrip-1",
            files=[
                AlbumQualityEvidenceFile(
                    relative_path="b.mp3",
                    size_bytes=2,
                    mtime_ns=2,
                    extension="mp3",
                    container="mp3",
                ),
                AlbumQualityEvidenceFile(
                    relative_path="a.mp3",
                    size_bytes=1,
                    mtime_ns=1,
                    extension="mp3",
                    container="mp3",
                ),
            ],
        )

        db.upsert_album_quality_evidence(evidence)
        loaded = db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )

        assert loaded is not None
        self.assertEqual(
            [file.relative_path for file in loaded.files],
            ["a.mp3", "b.mp3"],
        )
        assert loaded.id is not None
        loaded.files.append(AlbumQualityEvidenceFile(
            relative_path="mutated.mp3",
            size_bytes=3,
            mtime_ns=3,
            extension="mp3",
            container="mp3",
        ))
        reloaded = db.load_album_quality_evidence_by_id(loaded.id)
        assert reloaded is not None
        self.assertEqual(
            [file.relative_path for file in reloaded.files],
            ["a.mp3", "b.mp3"],
        )

    def test_album_quality_evidence_validates_snapshot(self):
        from lib.quality import AlbumQualityEvidenceFile

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42))
        with self.assertRaisesRegex(ValueError, "container is required"):
            db.upsert_album_quality_evidence(make_album_quality_evidence(
                mb_release_id="mb-validate-1",
                files=[
                    AlbumQualityEvidenceFile(
                        relative_path="bad.mp3",
                        size_bytes=1,
                        mtime_ns=1,
                        extension="mp3",
                        container="",
                    ),
                ],
            ))

    def test_album_quality_evidence_supports_download_log_addressing(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42))
        log_id = db.log_download(request_id=42, outcome="rejected")
        evidence = make_album_quality_evidence(mb_release_id="mb-dl-fk-1")

        db.upsert_album_quality_evidence(evidence)
        persisted = db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert persisted is not None and persisted.id is not None
        db.set_download_log_candidate_evidence(log_id, persisted.id)

        self.assertEqual(
            db.get_download_log_candidate_evidence_id(log_id),
            persisted.id,
        )
        loaded = db.load_album_quality_evidence_by_id(persisted.id)
        assert loaded is not None
        self.assertEqual(loaded.mb_release_id, "mb-dl-fk-1")

    def test_album_quality_evidence_supports_import_job_addressing(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42))
        job = db.enqueue_import_job(
            "manual_import",
            request_id=42,
            payload={"failed_path": "/tmp/candidate"},
        )
        evidence = make_album_quality_evidence(mb_release_id="mb-import-fk-1")

        db.upsert_album_quality_evidence(evidence)
        persisted = db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert persisted is not None and persisted.id is not None
        db.set_import_job_candidate_evidence(job.id, persisted.id)

        self.assertEqual(
            db.get_import_job_candidate_evidence_id(job.id),
            persisted.id,
        )

    def test_album_quality_evidence_supports_request_current_addressing(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42))
        evidence = make_album_quality_evidence(mb_release_id="mb-current-fk-1")
        db.upsert_album_quality_evidence(evidence)
        persisted = db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert persisted is not None and persisted.id is not None
        db.set_request_current_evidence(42, persisted.id)

        self.assertEqual(db.get_request_current_evidence_id(42), persisted.id)

    def test_album_quality_evidence_dedupes_by_content_key(self):
        """Upserting the same (mbid, fingerprint) twice keeps one row."""
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42))
        e = make_album_quality_evidence(mb_release_id="mb-dedupe-1")
        db.upsert_album_quality_evidence(e)
        db.upsert_album_quality_evidence(e)

        self.assertEqual(len(db.album_quality_evidence), 1)
        self.assertEqual(len(db._evidence_by_id), 1)

    def test_album_quality_evidence_preview_facts_mirror_pipeline_db(self):
        """U1: FakePipelineDB round-trips new preview-evidence facts the same
        way real PipelineDB does — every new field on AlbumQualityEvidence and
        the per-file decode_ok flag survives upsert/load.
        """
        import msgspec
        from lib.quality import AlbumQualityEvidenceFile

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42))
        evidence = make_album_quality_evidence(
            mb_release_id="mb-preview-facts-1",
            files=[
                AlbumQualityEvidenceFile(
                    relative_path="01 - Track.mp3",
                    size_bytes=1,
                    mtime_ns=1,
                    extension="mp3",
                    container="mp3",
                    decode_ok=False,
                ),
            ],
        )
        evidence = msgspec.structs.replace(
            evidence,
            audio_corrupt=True,
            folder_layout="nested",
            audio_file_count=1,
            filetype_band="mp3",
            matched_bad_audio_hash_id=99,
            matched_bad_audio_hash_path="01 - Track.mp3",
        )

        db.upsert_album_quality_evidence(evidence)
        loaded = db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert loaded is not None
        self.assertTrue(loaded.audio_corrupt)
        self.assertEqual(loaded.folder_layout, "nested")
        self.assertEqual(loaded.audio_file_count, 1)
        self.assertEqual(loaded.filetype_band, "mp3")
        self.assertEqual(loaded.matched_bad_audio_hash_id, 99)
        self.assertEqual(loaded.matched_bad_audio_hash_path, "01 - Track.mp3")
        self.assertFalse(loaded.files[0].decode_ok)

    def test_album_quality_evidence_empty_fileset_accepts_zero_count_on_fake(self):
        """U1 AE4: empty fileset with audio_file_count=0 is storable on fake too."""
        import msgspec

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42))
        evidence = make_album_quality_evidence(
            mb_release_id="mb-empty-1",
            files=[],
        )
        # default audio_file_count is 0 — explicit for clarity.
        evidence = msgspec.structs.replace(evidence, audio_file_count=0)
        db.upsert_album_quality_evidence(evidence)
        loaded = db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert loaded is not None
        self.assertEqual(loaded.audio_file_count, 0)
        self.assertEqual(loaded.files, [])

    def test_execute_records_calls_and_returns_queued_cursors(self):
        """``queue_execute_results`` registers a deterministic cursor sequence;
        each ``_execute`` call pops the next entry and records the call."""
        db = FakePipelineDB()
        cur1 = MagicMock(name="cur1")
        cur2 = MagicMock(name="cur2")
        db.queue_execute_results(cur1, cur2)

        result1 = db._execute("SELECT 1")
        result2 = db._execute("SELECT 2", (42,))

        self.assertIs(result1, cur1)
        self.assertIs(result2, cur2)
        self.assertEqual(
            db.execute_calls,
            [("SELECT 1", ()), ("SELECT 2", (42,))],
        )

    def test_execute_raises_when_queued_entry_is_exception(self):
        """Queued ``Exception`` entries are raised, not returned — replaces
        ``side_effect=[..., ProgrammingError(...), ...]`` from MagicMock."""
        db = FakePipelineDB()
        boom = RuntimeError("syntax error")
        db.queue_execute_results(MagicMock(), boom)

        db._execute("SELECT 1")
        with self.assertRaises(RuntimeError) as raised:
            db._execute("BOOM")
        self.assertIs(raised.exception, boom)

    def test_execute_with_empty_queue_returns_default(self):
        """Empty queue returns an empty cursor (production's "query ran,
        zero rows" shape) so tests that don't care about the cursor
        result can still call ``_execute`` without setup."""
        db = FakePipelineDB()
        self.assertEqual(db._execute("SELECT 1").fetchall(), [])
        self.assertEqual(db.execute_calls, [("SELECT 1", ())])

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

    def test_dashboard_wanted_total_includes_downloading(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="wanted"))
        db.seed_request(make_request_row(id=2, status="downloading"))
        db.seed_request(make_request_row(id=3, status="imported"))

        db.record_cycle_metrics(cycle_total_s=1.0)
        dashboard = db.get_pipeline_dashboard_metrics()

        self.assertEqual(db.cycle_metrics[0]["wanted_total"], 2)
        self.assertEqual(
            dashboard["coverage"]["wanted_trend"]["current_wanted"], 2)

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

    def test_update_download_state_if_downloading_guards_status(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="downloading"))
        db.seed_request(make_request_row(
            id=43,
            status="wanted",
            active_download_state={"filetype": "old"},
        ))

        updated = db.update_download_state_if_downloading(
            42,
            '{"filetype":"flac"}',
        )
        blocked = db.update_download_state_if_downloading(
            43,
            '{"filetype":"mp3"}',
        )

        self.assertTrue(updated)
        self.assertFalse(blocked)
        self.assertEqual(db.request(42)["active_download_state"], {"filetype": "flac"})
        self.assertEqual(db.request(43)["active_download_state"], {"filetype": "old"})

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

    def test_update_download_state_current_path_rewrites_nested_path(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            status="downloading",
            active_download_state={"filetype": "flac", "files": []},
        ))

        db.update_download_state_current_path(42, "/tmp/staged")

        row = db.request(42)
        self.assertEqual(row["active_download_state"]["current_path"], "/tmp/staged")

    def test_update_download_state_current_path_noop_when_not_downloading(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            status="imported",
            active_download_state=None,
        ))

        db.update_download_state_current_path(42, "/tmp/staged")

        row = db.request(42)
        self.assertEqual(row["status"], "imported")
        self.assertIsNone(row["active_download_state"])

    def test_update_download_state_current_path_noop_when_state_missing(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            status="downloading",
            active_download_state=None,
        ))

        db.update_download_state_current_path(42, "/tmp/staged")

        row = db.request(42)
        self.assertEqual(row["status"], "downloading")
        self.assertIsNone(row["active_download_state"])

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

    def test_plex_added_at_pin_add_get_pending_and_mark(self):
        """The fake mirrors migration-040 semantics: monotonic ids, pending
        filtered by status + captured_before cutoff, mark moves it terminal."""
        db = FakePipelineDB()
        now = datetime(2026, 6, 28, 12, 0, tzinfo=timezone.utc)
        pin_id = db.add_plex_added_at_pin(
            imported_path="Muse/2026 - The Wow! Signal",
            original_added_at=1782611948,
            rating_key="458495",
            request_id=8812,
        )
        self.assertEqual(pin_id, 1)
        # Force a deterministic capture time in the past, then read pending.
        db.plex_added_at_pins[0]["captured_at"] = now - timedelta(minutes=10)
        pending = db.get_pending_plex_added_at_pins(captured_before=now, limit=100)
        self.assertEqual(len(pending), 1)
        row = pending[0]
        self.assertEqual(row["original_added_at"], 1782611948)
        self.assertEqual(row["rating_key"], "458495")
        self.assertEqual(row["request_id"], 8812)
        self.assertEqual(row["status"], "pending")
        # A cutoff before the capture excludes the pin (settle-window guard).
        self.assertEqual(
            db.get_pending_plex_added_at_pins(
                captured_before=now - timedelta(hours=1), limit=100),
            [])
        # Marking terminal removes it from pending.
        db.mark_plex_added_at_pin(pin_id, status="done", reconciled_at=now)
        self.assertEqual(
            db.get_pending_plex_added_at_pins(captured_before=now, limit=100), [])
        self.assertEqual(db.plex_added_at_pins[0]["status"], "done")
        self.assertEqual(db.plex_added_at_pins[0]["reconciled_at"], now)

    def test_log_download_rejects_non_canonical_outcome(self):
        """Mirror of download_log_outcome_check — the fake must reject
        exactly what production rejects (test-fidelity Rule A/B; the
        #146 grace escape shipped outcome='error' past a permissive
        fake and crashed on the real CHECK constraint)."""
        import psycopg2.errors
        db = FakePipelineDB()
        with self.assertRaises(psycopg2.errors.CheckViolation):
            db.log_download(42, outcome="error")

    def test_set_update_download_state_error_raises_and_leaves_row_untouched(self):
        """Issue #564 review: the injection seam mirrors a psycopg2 error
        at the UPDATE — raises from BOTH state writers, records the
        attempt, never mutates the row; other requests are unaffected."""
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=1, status="downloading",
            active_download_state={"original": True}))
        db.seed_request(make_request_row(
            id=2, status="downloading", mb_release_id="mbid-2"))
        boom = RuntimeError("UPDATE failed")
        db.set_update_download_state_error(1, boom)

        with self.assertRaises(RuntimeError):
            db.update_download_state(1, '{"mutated": true}')
        with self.assertRaises(RuntimeError):
            db.update_download_state_if_downloading(1, '{"mutated": true}')

        # Row 1 untouched; both attempts recorded.
        self.assertEqual(
            db.request(1)["active_download_state"], {"original": True})
        self.assertEqual(len(db.update_download_state_calls), 2)
        # Other requests still write normally.
        self.assertTrue(
            db.update_download_state_if_downloading(2, '{"ok": true}'))
        self.assertEqual(
            db.request(2)["active_download_state"], {"ok": True})

    def test_log_download_records_transfer_detail(self):
        """Issue #564 C7: transfer_detail is a first-class field on
        DownloadLogRow, not swallowed into .extra."""
        db = FakePipelineDB()
        detail = [
            {"username": "user1", "filename": "01.flac",
             "last_state": "Completed, Errored",
             "last_exception": "Read error: Connection reset by peer",
             "bytes_transferred": 0, "retry_count": 2},
        ]
        db.log_download(42, outcome="timeout", transfer_detail=detail)

        self.assertEqual(db.download_logs[0].transfer_detail, detail)

    def test_assert_log_passes(self):
        db = FakePipelineDB()
        log_id = db.log_download(42, outcome="success", soulseek_username="user1")

        # Should not raise
        self.assertEqual(log_id, db.download_logs[0].id)
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

    def test_reset_to_wanted_clears_manual_reason(self):
        """U6 fake parity: re-queue clears ``manual_reason`` and counters."""
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42, status="manual", manual_reason="search_exhausted",
            search_attempts=7,
        ))

        db.reset_to_wanted(42)

        row = db.request(42)
        self.assertEqual(row["status"], "wanted")
        self.assertEqual(row["search_attempts"], 0)
        self.assertIsNone(row["manual_reason"])


# ---------------------------------------------------------------------------
# Field resolutions (migration 030) — fake parity
# ---------------------------------------------------------------------------


class TestFakePipelineDBFieldResolutions(unittest.TestCase):
    """FakePipelineDB mirrors the ``album_request_field_resolutions`` UPSERT
    semantics. Tests asserting on side-table state use this fake; the real
    PipelineDB integration is exercised in ``tests/test_pipeline_db.py``.
    """

    def test_first_call_creates_row_with_attempts_one(self):
        db = FakePipelineDB()
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
        db = FakePipelineDB()
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
        db = FakePipelineDB()
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
        db = FakePipelineDB()
        self.assertIsNone(db.get_field_resolution(42, "release_group_year"))


# ---------------------------------------------------------------------------
# Triage cohort fakes (U15)
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Persisted search plans (U1) — fake parity
# ---------------------------------------------------------------------------


class TestFakePipelineDBSearchPlans(unittest.TestCase):
    """FakePipelineDB mirrors the U1 plan methods with the same semantics
    so tests that exercise plan generation, reconciliation, consumed
    attempts, and stale completions can run without a real Postgres.
    """

    def _items(self, *queries: str):
        from lib.pipeline_db import SearchPlanItemInput
        return [
            SearchPlanItemInput(
                ordinal=i,
                strategy=f"slot_{i}",
                query=q,
                canonical_query_key=q.lower(),
            )
            for i, q in enumerate(queries)
        ]

    def test_successful_plan_sets_active_and_resets_cursor(self):
        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="A", album_title="B", source="request",
            mb_release_id="m1",
        )
        plan_id = db.create_successful_search_plan(
            request_id=rid,
            generator_id="g1",
            items=self._items("Q0", "Q1"),
        )
        active = db.get_active_search_plan(rid)
        assert active is not None
        self.assertEqual(active.plan.id, plan_id)
        self.assertEqual(active.next_ordinal, 0)
        self.assertEqual(active.cycle_count, 0)
        self.assertEqual(len(active.items), 2)
        self.assertEqual(active.items[0].ordinal, 0)
        self.assertEqual(active.items[1].ordinal, 1)
        self.assertEqual(db.request(rid)["active_plan_id"], plan_id)

    def test_failed_deterministic_plan_keeps_request_unsearchable(self):
        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="A", album_title="B", source="request",
            mb_release_id="m1",
        )
        plan_id = db.create_failed_search_plan(
            request_id=rid, generator_id="g1",
            failure_class="no_runnable_query", transient=False,
        )
        self.assertIsNone(db.get_active_search_plan(rid))
        self.assertEqual(
            db.search_plans[plan_id].status, "failed_deterministic")
        self.assertEqual(db.request(rid)["status"], "wanted")
        self.assertIsNone(db.request(rid)["active_plan_id"])

    def test_failed_transient_plan_is_visible_and_not_sticky(self):
        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="A", album_title="B", source="request",
            mb_release_id="m1",
        )
        pid = db.create_failed_search_plan(
            request_id=rid, generator_id="g1",
            failure_class="resolver_unavailable", transient=True,
        )
        self.assertEqual(db.search_plans[pid].status, "failed_transient")

    def test_supersede_replaces_active_and_resets_cursor(self):
        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="A", album_title="B", source="request",
            mb_release_id="m1",
        )
        first = db.create_successful_search_plan(
            request_id=rid, generator_id="g1",
            items=self._items("Q0", "Q1"),
        )
        # Move cursor away from (0, 0) so we can prove reset.
        db.update_request_fields(rid, next_plan_ordinal=1, plan_cycle_count=4)
        new_id = db.supersede_search_plan_with_replacement(
            request_id=rid, generator_id="g2",
            items=self._items("Q2"),
        )
        active = db.get_active_search_plan(rid)
        assert active is not None
        self.assertEqual(active.plan.id, new_id)
        self.assertEqual(active.next_ordinal, 0)
        self.assertEqual(active.cycle_count, 0)
        # Old plan is superseded with a back-link.
        old = db.search_plans[first]
        self.assertEqual(old.status, "superseded")
        self.assertIsNotNone(old.superseded_at)
        self.assertEqual(old.superseded_by_plan_id, new_id)

    def test_list_wanted_for_plan_reconciliation_ignores_pagination(self):
        db = FakePipelineDB()
        rid_planned = db.add_request(
            artist_name="A", album_title="B", source="request",
            mb_release_id="planned",
        )
        rid_unplanned = db.add_request(
            artist_name="A", album_title="C", source="request",
            mb_release_id="unplanned",
        )
        rid_imported = db.add_request(
            artist_name="A", album_title="D", source="request",
            mb_release_id="imported",
        )
        db.update_status(rid_imported, "imported")
        db.create_successful_search_plan(
            request_id=rid_planned, generator_id="g1",
            items=self._items("Q"),
        )
        rows = db.list_wanted_for_plan_reconciliation()
        rids = {r.request_id for r in rows}
        self.assertEqual(rids, {rid_planned, rid_unplanned})
        by_id = {r.request_id: r for r in rows}
        self.assertEqual(by_id[rid_planned].active_plan_generator_id, "g1")
        self.assertIsNone(by_id[rid_unplanned].active_plan_generator_id)

    def test_inspection_returns_active_failed_superseded_legacy(self):
        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="A", album_title="B", source="request",
            mb_release_id="m1",
        )
        # Legacy log row (no plan context).
        db.log_search(rid, query="legacy", outcome="error")
        det = db.create_failed_search_plan(
            request_id=rid, generator_id="g1",
            failure_class="no_runnable_query", transient=False,
        )
        trans = db.create_failed_search_plan(
            request_id=rid, generator_id="g1",
            failure_class="resolver_unavailable", transient=True,
        )
        db.create_successful_search_plan(
            request_id=rid, generator_id="g1",
            items=self._items("Q0"),
        )
        new_id = db.supersede_search_plan_with_replacement(
            request_id=rid, generator_id="g2",
            items=self._items("Q1"),
        )
        info = db.get_search_plan_inspection(rid)
        assert info.active is not None
        self.assertEqual(info.active.plan.id, new_id)
        assert info.latest_failed_deterministic is not None
        self.assertEqual(info.latest_failed_deterministic.id, det)
        assert info.latest_failed_transient is not None
        self.assertEqual(info.latest_failed_transient.id, trans)
        self.assertEqual(info.superseded_count, 1)
        self.assertEqual(info.legacy_search_log_count, 1)

    def test_consumed_attempt_advances_cursor_and_writes_log(self):
        from lib.pipeline_db import ConsumedAttemptInput
        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="A", album_title="B", source="request",
            mb_release_id="m1",
        )
        plan_id = db.create_successful_search_plan(
            request_id=rid, generator_id="g1",
            items=self._items("Q0", "Q1"),
        )
        active = db.get_active_search_plan(rid)
        assert active is not None
        result = db.record_consumed_search_attempt(ConsumedAttemptInput(
            request_id=rid, plan_id=plan_id,
            plan_item_id=active.items[0].id, plan_ordinal=0,
            plan_strategy="slot_0", plan_canonical_query_key="q0",
            plan_repeat_group=None, plan_generator_id="g1", query="Q0",
            outcome="no_match", plan_item_count=2,
            apply_scheduler_attempt=True, scheduler_success=False,
        ))
        self.assertEqual(result.cursor_update_status, "advanced")
        self.assertEqual(result.new_next_ordinal, 1)
        self.assertEqual(result.new_cycle_count, 0)
        self.assertFalse(result.is_stale)
        self.assertEqual(db.request(rid)["next_plan_ordinal"], 1)
        # Log row carries plan context + cycle snapshot.
        log = db.search_logs[0]
        self.assertEqual(log.plan_id, plan_id)
        self.assertEqual(log.plan_ordinal, 0)
        self.assertEqual(log.execution_stage, "accepted")
        self.assertTrue(log.attempt_consumed)
        self.assertEqual(log.cursor_update_status, "advanced")
        self.assertEqual(log.plan_cycle_snapshot, 0)
        # Scheduler/backoff applied.
        self.assertEqual(db.request(rid)["search_attempts"], 1)
        self.assertIsNotNone(db.request(rid)["next_retry_after"])

    def test_consumed_attempt_wraps_at_final_ordinal_and_increments_cycle(self):
        from lib.pipeline_db import ConsumedAttemptInput
        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="A", album_title="B", source="request",
            mb_release_id="m1",
        )
        plan_id = db.create_successful_search_plan(
            request_id=rid, generator_id="g1",
            items=self._items("Q0", "Q1"),
        )
        active = db.get_active_search_plan(rid)
        assert active is not None
        db.update_request_fields(rid, next_plan_ordinal=1)
        result = db.record_consumed_search_attempt(ConsumedAttemptInput(
            request_id=rid, plan_id=plan_id,
            plan_item_id=active.items[1].id, plan_ordinal=1,
            plan_strategy="slot_1", plan_canonical_query_key="q1",
            plan_repeat_group=None, plan_generator_id="g1", query="Q1",
            outcome="found", plan_item_count=2,
            apply_scheduler_attempt=True, scheduler_success=True,
        ))
        self.assertEqual(result.cursor_update_status, "wrapped")
        self.assertEqual(result.new_next_ordinal, 0)
        self.assertEqual(result.new_cycle_count, 1)
        self.assertEqual(db.request(rid)["plan_cycle_count"], 1)
        # success path doesn't bump search_attempts.
        self.assertEqual(db.request(rid)["search_attempts"], 0)

    def test_u12_fake_writes_failure_class_at_wrap(self):
        """FakePipelineDB mirrors the real wrap-time classification write."""
        from lib.pipeline_db import ConsumedAttemptInput
        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="A", album_title="B", source="request",
            mb_release_id="m1",
        )
        plan_id = db.create_successful_search_plan(
            request_id=rid, generator_id="g1",
            items=self._items("Q0", "Q1"),
        )
        active = db.get_active_search_plan(rid)
        assert active is not None
        # Cycle 0: both items return no_match → all-candidates-no-match.
        db.record_consumed_search_attempt(ConsumedAttemptInput(
            request_id=rid, plan_id=plan_id,
            plan_item_id=active.items[0].id, plan_ordinal=0,
            plan_strategy="slot_0", plan_canonical_query_key="q0",
            plan_repeat_group=None, plan_generator_id="g1", query="Q0",
            outcome="no_match", plan_item_count=2,
            rejection_reason="strict_count_mismatch",
        ))
        result = db.record_consumed_search_attempt(ConsumedAttemptInput(
            request_id=rid, plan_id=plan_id,
            plan_item_id=active.items[1].id, plan_ordinal=1,
            plan_strategy="slot_1", plan_canonical_query_key="q1",
            plan_repeat_group=None, plan_generator_id="g1", query="Q1",
            outcome="no_match", plan_item_count=2,
            rejection_reason="avg_ratio_low",
        ))
        self.assertEqual(result.cursor_update_status, "wrapped")
        self.assertEqual(db.request(rid)["failure_class"],
                         "B_cands_never_match")

    def test_u12_fake_does_not_overwrite_failure_class_when_classifier_none(
        self,
    ):
        """Degenerate wrap (zero consumed attempts in cycle) preserves prior."""
        from lib.pipeline_db import (CURSOR_UPDATE_WRAPPED,
                                     ConsumedAttemptInput)
        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="A", album_title="B", source="request",
            mb_release_id="m1",
        )
        plan_id = db.create_successful_search_plan(
            request_id=rid, generator_id="g1",
            items=self._items("Q0"),
        )
        active = db.get_active_search_plan(rid)
        assert active is not None
        # Seed a prior failure_class. Build a wrap whose consumed
        # attempts are all on cycle N-1 (i.e. zero attempts on cycle
        # we're wrapping). We simulate this by directly tampering with
        # the search_log row's plan_cycle_snapshot post-insert so the
        # classifier's per-cycle filter excludes the only row.
        db.update_request_fields(rid, failure_class="E_mixed")
        result = db.record_consumed_search_attempt(ConsumedAttemptInput(
            request_id=rid, plan_id=plan_id,
            plan_item_id=active.items[0].id, plan_ordinal=0,
            plan_strategy="slot_0", plan_canonical_query_key="q0",
            plan_repeat_group=None, plan_generator_id="g1", query="Q0",
            outcome="found", plan_item_count=1,
        ))
        self.assertEqual(result.cursor_update_status, CURSOR_UPDATE_WRAPPED)
        # The single attempt was found+wanted → D, which overwrites E.
        self.assertEqual(db.request(rid)["failure_class"],
                         "D_found_but_no_import")

    def test_u12_fake_classifies_resolved_when_status_not_wanted(self):
        """Status moved past 'wanted' mid-cycle → resolved verdict on wrap."""
        from lib.pipeline_db import ConsumedAttemptInput
        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="A", album_title="B", source="request",
            mb_release_id="m1", status="imported",
        )
        plan_id = db.create_successful_search_plan(
            request_id=rid, generator_id="g1",
            items=self._items("Q0"),
        )
        active = db.get_active_search_plan(rid)
        assert active is not None
        result = db.record_consumed_search_attempt(ConsumedAttemptInput(
            request_id=rid, plan_id=plan_id,
            plan_item_id=active.items[0].id, plan_ordinal=0,
            plan_strategy="slot_0", plan_canonical_query_key="q0",
            plan_repeat_group=None, plan_generator_id="g1", query="Q0",
            outcome="no_match", plan_item_count=1,
        ))
        self.assertEqual(result.cursor_update_status, "wrapped")
        self.assertEqual(db.request(rid)["failure_class"], "resolved")

    def test_u12_fake_does_not_write_on_plain_advance(self):
        """Classification only on wrap, not on plain advance."""
        from lib.pipeline_db import ConsumedAttemptInput
        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="A", album_title="B", source="request",
            mb_release_id="m1",
        )
        plan_id = db.create_successful_search_plan(
            request_id=rid, generator_id="g1",
            items=self._items("Q0", "Q1"),
        )
        active = db.get_active_search_plan(rid)
        assert active is not None
        result = db.record_consumed_search_attempt(ConsumedAttemptInput(
            request_id=rid, plan_id=plan_id,
            plan_item_id=active.items[0].id, plan_ordinal=0,
            plan_strategy="slot_0", plan_canonical_query_key="q0",
            plan_repeat_group=None, plan_generator_id="g1", query="Q0",
            outcome="no_match", plan_item_count=2,
        ))
        self.assertEqual(result.cursor_update_status, "advanced")
        self.assertIsNone(db.request(rid)["failure_class"])

    def test_u12_fake_rolls_back_failure_class_on_validation_failure(self):
        """A txn rollback must restore failure_class to the pre-call value."""
        from lib.pipeline_db import ConsumedAttemptInput
        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="A", album_title="B", source="request",
            mb_release_id="m1",
        )
        plan_id = db.create_successful_search_plan(
            request_id=rid, generator_id="g1",
            items=self._items("Q0"),
        )
        # Seed a prior verdict so we can prove rollback restores it.
        db.update_request_fields(rid, failure_class="A_zero_results_dominant")
        # plan_item_id 999_999 does not belong to plan_id → fake raises;
        # the whole transaction rolls back, including any speculative
        # failure_class write that might have happened.
        with self.assertRaises(Exception):
            db.record_consumed_search_attempt(ConsumedAttemptInput(
                request_id=rid, plan_id=plan_id,
                plan_item_id=999999, plan_ordinal=0,
                plan_strategy="slot_0", plan_canonical_query_key="q0",
                plan_repeat_group=None, plan_generator_id="g1", query="Q0",
                outcome="no_match", plan_item_count=1,
            ))
        self.assertEqual(db.request(rid)["failure_class"],
                         "A_zero_results_dominant")
        self.assertEqual(db.search_logs, [])

    def test_consumed_attempt_stale_when_request_already_advanced(self):
        from lib.pipeline_db import ConsumedAttemptInput
        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="A", album_title="B", source="request",
            mb_release_id="m1",
        )
        plan_id = db.create_successful_search_plan(
            request_id=rid, generator_id="g1",
            items=self._items("Q0", "Q1"),
        )
        active = db.get_active_search_plan(rid)
        assert active is not None
        # Mid-flight regeneration / out-of-band advance.
        db.update_request_fields(rid, next_plan_ordinal=1)
        result = db.record_consumed_search_attempt(ConsumedAttemptInput(
            request_id=rid, plan_id=plan_id,
            plan_item_id=active.items[0].id, plan_ordinal=0,
            plan_strategy="slot_0", plan_canonical_query_key="q0",
            plan_repeat_group=None, plan_generator_id="g1", query="Q0",
            outcome="found", plan_item_count=2,
            apply_scheduler_attempt=True, scheduler_success=True,
        ))
        self.assertTrue(result.is_stale)
        self.assertEqual(result.cursor_update_status, "stale")
        # Cursor unchanged.
        self.assertEqual(db.request(rid)["next_plan_ordinal"], 1)
        # Log row is still inserted, marked stale.
        log = db.search_logs[0]
        self.assertEqual(log.execution_stage, "stale_completion")
        self.assertFalse(log.attempt_consumed)
        self.assertEqual(log.cursor_update_status, "stale")
        self.assertEqual(log.stale_reason, "regenerated")
        # No scheduler bump on stale.
        self.assertEqual(db.request(rid)["search_attempts"], 0)

    def test_consumed_attempt_stale_when_cycle_changed(self):
        from lib.pipeline_db import ConsumedAttemptInput
        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="A", album_title="B", source="request",
            mb_release_id="m1",
        )
        plan_id = db.create_successful_search_plan(
            request_id=rid, generator_id="g1",
            items=self._items("Q0"),
        )
        active = db.get_active_search_plan(rid)
        assert active is not None
        db.update_request_fields(rid, plan_cycle_count=1)
        result = db.record_consumed_search_attempt(ConsumedAttemptInput(
            request_id=rid, plan_id=plan_id,
            plan_item_id=active.items[0].id, plan_ordinal=0,
            plan_strategy="slot_0", plan_canonical_query_key="q0",
            plan_repeat_group=None, plan_generator_id="g1", query="Q0",
            outcome="found", plan_item_count=1,
            cycle_count_snapshot=0,
            apply_scheduler_attempt=True, scheduler_success=True,
        ))

        self.assertTrue(result.is_stale)
        self.assertEqual(result.cursor_update_status, "stale")
        self.assertEqual(db.request(rid)["plan_cycle_count"], 1)
        log = db.search_logs[0]
        self.assertEqual(log.execution_stage, "stale_completion")
        self.assertFalse(log.attempt_consumed)
        self.assertEqual(log.plan_cycle_snapshot, 0)

    def test_consumed_attempt_rolls_back_on_validation_failure(self):
        from lib.pipeline_db import ConsumedAttemptInput
        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="A", album_title="B", source="request",
            mb_release_id="m1",
        )
        plan_id = db.create_successful_search_plan(
            request_id=rid, generator_id="g1",
            items=self._items("Q0"),
        )
        # plan_item_id 999_999 does not belong to plan_id; the fake mirrors
        # the real DB FK violation by raising. Either way, no log row may
        # land and the cursor must stay put.
        with self.assertRaises(Exception):
            db.record_consumed_search_attempt(ConsumedAttemptInput(
                request_id=rid, plan_id=plan_id,
                plan_item_id=999999, plan_ordinal=0,
                plan_strategy="slot_0", plan_canonical_query_key="q0",
                plan_repeat_group=None, plan_generator_id="g1",
                query="Q0", outcome="no_match", plan_item_count=1,
            ))
        self.assertEqual(db.search_logs, [])
        self.assertEqual(db.request(rid)["next_plan_ordinal"], 0)

    def test_consumed_attempt_rejects_item_from_another_request(self):
        from lib.pipeline_db import ConsumedAttemptInput
        db = FakePipelineDB()
        rid_a = db.add_request(
            artist_name="A", album_title="B", source="request",
            mb_release_id="m1",
        )
        rid_b = db.add_request(
            artist_name="C", album_title="D", source="request",
            mb_release_id="m2",
        )
        plan_a = db.create_successful_search_plan(
            request_id=rid_a, generator_id="g1", items=self._items("Q0"))
        plan_b = db.create_successful_search_plan(
            request_id=rid_b, generator_id="g1", items=self._items("R0"))
        item_b = next(
            it for it in db.search_plan_items.values()
            if it.plan_id == plan_b)

        with self.assertRaises(ValueError):
            db.record_consumed_search_attempt(ConsumedAttemptInput(
                request_id=rid_a, plan_id=plan_a,
                plan_item_id=item_b.id, plan_ordinal=0,
                plan_strategy="slot_0", plan_canonical_query_key="q0",
                plan_repeat_group=None, plan_generator_id="g1",
                query="Q0", outcome="no_match", plan_item_count=1,
            ))
        self.assertEqual(db.search_logs, [])

    def test_non_consuming_logs_and_applies_backoff(self):
        from lib.pipeline_db import NonConsumingAttemptInput
        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="A", album_title="B", source="request",
            mb_release_id="m1",
        )
        log_id = db.record_non_consuming_search_attempt(
            NonConsumingAttemptInput(
                request_id=rid, outcome="error",
                error_message="slskd 503",
                apply_scheduler_attempt=True,
            )
        )
        self.assertGreater(log_id, 0)
        log = db.search_logs[0]
        self.assertEqual(log.execution_stage, "pre_attempt")
        self.assertFalse(log.attempt_consumed)
        self.assertEqual(log.cursor_update_status, "unchanged")
        self.assertEqual(db.request(rid)["next_plan_ordinal"], 0)
        self.assertEqual(db.request(rid)["search_attempts"], 1)
        self.assertIsNotNone(db.request(rid)["next_retry_after"])

    def test_request_delete_cascades_plans_and_items(self):
        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="A", album_title="B", source="request",
            mb_release_id="m1",
        )
        plan_id = db.create_successful_search_plan(
            request_id=rid, generator_id="g1",
            items=self._items("Q0"),
        )
        # Make sure items are present pre-delete.
        self.assertTrue(any(
            it.plan_id == plan_id for it in db.search_plan_items.values()))
        db.delete_request(rid)
        self.assertNotIn(plan_id, db.search_plans)
        self.assertFalse(any(
            it.plan_id == plan_id for it in db.search_plan_items.values()))


class TestFakeGetWantedSearchable(unittest.TestCase):
    """``FakePipelineDB.get_wanted_searchable`` mirrors PipelineDB's
    plan-aware execution-eligibility filter.
    """

    def _items(self, *queries: str):
        from lib.pipeline_db import SearchPlanItemInput
        return [
            SearchPlanItemInput(ordinal=i, strategy="default", query=q)
            for i, q in enumerate(queries)
        ]

    def _make_active(self, db, rid, gen):
        return db.create_successful_search_plan(
            request_id=rid, generator_id=gen, items=self._items("Q"))

    def test_filters_to_current_generator_active_plans(self):
        db = FakePipelineDB()
        rid_match = db.add_request(
            artist_name="A", album_title="B", source="request",
            mb_release_id="match")
        self._make_active(db, rid_match, "g1")

        rid_no_plan = db.add_request(
            artist_name="A", album_title="C", source="request",
            mb_release_id="no-plan")

        rid_old = db.add_request(
            artist_name="A", album_title="D", source="request",
            mb_release_id="old")
        self._make_active(db, rid_old, "g0")

        rid_imp = db.add_request(
            artist_name="A", album_title="E", source="request",
            mb_release_id="imp")
        self._make_active(db, rid_imp, "g1")
        db.update_status(rid_imp, "imported")

        rids = {r["id"] for r in db.get_wanted_searchable("g1")}
        self.assertEqual(rids, {rid_match})
        # Sanity: rid_no_plan and rid_old are visible to non-plan
        # diagnostic ``get_wanted`` though.
        all_ids = {r["id"] for r in db.get_wanted()}
        self.assertIn(rid_no_plan, all_ids)
        self.assertIn(rid_old, all_ids)

    def test_failed_plans_excluded(self):
        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="A", album_title="B", source="request",
            mb_release_id="fd")
        db.create_failed_search_plan(
            request_id=rid, generator_id="g1",
            failure_class="no_runnable_query", transient=False,
        )
        self.assertEqual(db.get_wanted_searchable("g1"), [])

        rid2 = db.add_request(
            artist_name="A", album_title="C", source="request",
            mb_release_id="ft")
        db.create_failed_search_plan(
            request_id=rid2, generator_id="g1",
            failure_class="resolver_unavailable", transient=True,
        )
        self.assertEqual(db.get_wanted_searchable("g1"), [])

    def test_respects_retry_backoff(self):
        from datetime import datetime, timedelta, timezone
        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="A", album_title="B", source="request",
            mb_release_id="bo")
        self._make_active(db, rid, "g1")
        db.update_request_fields(
            rid,
            next_retry_after=datetime.now(timezone.utc) + timedelta(hours=1),
        )
        self.assertEqual(db.get_wanted_searchable("g1"), [])

    def test_active_youtube_rescue_excluded(self):
        from lib.import_queue import (
            IMPORT_JOB_YOUTUBE,
            youtube_import_dedupe_key,
            youtube_import_payload,
        )

        db = FakePipelineDB()
        rid_running = db.add_request(
            artist_name="A", album_title="B", source="request",
            mb_release_id="yt-running")
        self._make_active(db, rid_running, "g1")
        db.insert_youtube_running(
            request_id=rid_running,
            browse_id="MPREb_running",
            audio_playlist_id=None,
            yt_url="https://music.youtube.com/playlist?list=running",
            expected_track_count=10,
        )

        rid_import = db.add_request(
            artist_name="A", album_title="C", source="request",
            mb_release_id="yt-import")
        self._make_active(db, rid_import, "g1")
        db.enqueue_import_job(
            IMPORT_JOB_YOUTUBE,
            request_id=rid_import,
            dedupe_key=youtube_import_dedupe_key(123),
            payload=youtube_import_payload(
                staged_path="/tmp/yt-import",
                request_id=rid_import,
                browse_id="MPREb_import",
            ),
        )

        rid_clear = db.add_request(
            artist_name="A", album_title="D", source="request",
            mb_release_id="clear")
        self._make_active(db, rid_clear, "g1")

        self.assertEqual(
            {r["id"] for r in db.get_wanted_searchable("g1")},
            {rid_clear},
        )


class TestFakePipelineDBSearchPlanContract(unittest.TestCase):
    """Lightweight signature parity check between PipelineDB and
    FakePipelineDB for U1 methods. Catches drift when a real DB method
    grows a new keyword and the fake forgets to mirror it.
    """

    METHODS = (
        "create_successful_search_plan",
        "create_failed_search_plan",
        "supersede_search_plan_with_replacement",
        "get_active_search_plan",
        "get_wanted_searchable",
        "list_wanted_for_plan_reconciliation",
        "list_search_plan_classification_for_requests",
        "get_search_plan_inspection",
        "get_search_plan_stats",
        "get_search_plan_stats_history",
        "get_legacy_search_log_summary",
        "get_search_history_page",
        "record_consumed_search_attempt",
        "record_non_consuming_search_attempt",
    )

    def test_fake_method_signatures_match_real(self):
        for name in self.METHODS:
            with self.subTest(method=name):
                real_sig = inspect.signature(
                    getattr(PipelineDB, name))
                fake_sig = inspect.signature(
                    getattr(FakePipelineDB, name))
                self.assertEqual(
                    list(real_sig.parameters.keys()),
                    list(fake_sig.parameters.keys()),
                    f"FakePipelineDB.{name} drifted from "
                    f"PipelineDB.{name}",
                )


class TestFakeSlskdAPI(unittest.TestCase):
    def test_get_downloads_returns_queued_snapshots(self):
        """#507: get_all_downloads() now runs the raw JSON snapshot through
        parse_downloads_envelope(), the same as production — mirroring the
        real decode is the point (test-fidelity Rule B)."""
        from lib.slskd_client import parse_downloads_envelope
        first = [{"username": "user1", "directories": [{"files": []}]}]
        second = [{"username": "user1", "directories": [{"files": [
            {"filename": "track.mp3", "id": "tid-1"},
        ]}]}]
        slskd = FakeSlskdAPI(download_snapshots=[first, second])

        self.assertEqual(
            slskd.transfers.get_all_downloads(includeRemoved=True),
            parse_downloads_envelope(first))
        self.assertEqual(
            slskd.transfers.get_all_downloads(includeRemoved=True),
            parse_downloads_envelope(second))
        self.assertEqual(
            slskd.transfers.get_all_downloads(includeRemoved=True),
            parse_downloads_envelope(second))
        self.assertEqual(slskd.transfers.get_all_downloads_calls, [True, True, True])

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

    def test_user_status_default_is_online(self):
        """Unset users default to Online so legacy tests stay green."""
        slskd = FakeSlskdAPI()

        result = slskd.users.status("never_set")

        self.assertEqual(result["presence"], "Online")
        self.assertEqual(slskd.users.status_calls, ["never_set"])

    def test_user_status_returns_configured_presence(self):
        slskd = FakeSlskdAPI()
        slskd.users.set_status("alice", "Online")
        slskd.users.set_status("bob", "Away")
        slskd.users.set_status("carol", "Offline")

        self.assertEqual(slskd.users.status("alice")["presence"], "Online")
        self.assertEqual(slskd.users.status("bob")["presence"], "Away")
        self.assertEqual(slskd.users.status("carol")["presence"], "Offline")
        self.assertEqual(
            slskd.users.status_calls, ["alice", "bob", "carol"],
        )

    def test_user_status_raises_configured_error(self):
        slskd = FakeSlskdAPI()
        boom = RuntimeError("slskd unreachable")
        slskd.users.set_status_error("flaky", boom)

        with self.assertRaises(RuntimeError):
            slskd.users.status("flaky")
        # The call is still recorded so tests can assert ordering.
        self.assertEqual(slskd.users.status_calls, ["flaky"])

    def test_user_status_payload_shape_matches_slskd_api(self):
        """Returned dict mirrors slskd-api UserStatus TypedDict shape:
        {presence: str, isPrivileged: bool}."""
        slskd = FakeSlskdAPI()
        slskd.users.set_status("alice", "Online")

        result = slskd.users.status("alice")

        self.assertIn("presence", result)
        self.assertIn("isPrivileged", result)
        self.assertIsInstance(result["isPrivileged"], bool)

    def test_add_transfer_can_carry_exception_reason(self):
        """Issue #564: seeded transfers can carry slskd's real failure
        reason so poll/harvest tests can drive it through the same
        parse_downloads_envelope() decode production uses."""
        slskd = FakeSlskdAPI()
        slskd.add_transfer(
            username="user1", directory="user1\\Music",
            filename="user1\\Music\\01.flac", id="tid-1",
            state="Completed, Rejected",
            exception="Transfer rejected: Banned",
        )

        downloads = slskd.transfers.get_all_downloads(includeRemoved=True)

        snap = downloads[0].directories[0].files[0]
        self.assertEqual(snap.exception, "Transfer rejected: Banned")


class TestFakeSlskdSearches(unittest.TestCase):
    """Self-test for the FakeSlskdSearches stub introduced in U5."""

    def test_search_text_records_kwargs_and_returns_id(self):
        slskd = FakeSlskdAPI()
        slskd.searches.search_text_id_sequence = [101]
        result = slskd.searches.search_text(
            searchText="*rtist Album",
            searchTimeout=30000,
            filterResponses=True,
            maximumPeerQueueLength=5,
            minimumPeerUploadSpeed=0,
            responseLimit=1000,
        )
        self.assertEqual(result, {"id": 101})
        call = slskd.searches.search_text_calls[0]
        self.assertEqual(call.search_text, "*rtist Album")
        self.assertEqual(call.kwargs["responseLimit"], 1000)
        self.assertEqual(call.kwargs["searchTimeout"], 30000)

    def test_state_returns_canned_terminal_state(self):
        slskd = FakeSlskdAPI()
        slskd.searches.add_search(search_id=7, state="ResponseLimitReached")

        state = slskd.searches.state(7, False)

        self.assertEqual(state["state"], "ResponseLimitReached")
        self.assertEqual(slskd.searches.state_calls, [(7, False)])

    def test_search_responses_returns_canned_payload(self):
        slskd = FakeSlskdAPI()
        responses = [
            {"username": "u1", "uploadSpeed": 100, "files": [
                {"filename": "u1\\Music\\01.flac"},
            ]},
        ]
        slskd.searches.add_search(search_id=11, responses=responses)

        out = slskd.searches.search_responses(11)

        self.assertEqual(out, responses)
        # Response list must be a deep copy — tests can mutate freely.
        out[0]["files"].append({"filename": "tampered.flac"})
        again = slskd.searches.search_responses(11)
        self.assertEqual(len(again[0]["files"]), 1)

    def test_search_text_error_propagates(self):
        slskd = FakeSlskdAPI()
        slskd.searches.search_text_error = RuntimeError("slskd offline")
        with self.assertRaises(RuntimeError):
            slskd.searches.search_text(searchText="x", responseLimit=1000)

    def test_unknown_search_id_returns_completed_with_no_responses(self):
        slskd = FakeSlskdAPI()
        # No add_search() call — the fake should still answer politely.
        state = slskd.searches.search_text(
            searchText="x", responseLimit=1000)
        sid = state["id"]
        self.assertEqual(slskd.searches.state(sid)["state"], "Completed")
        self.assertEqual(slskd.searches.search_responses(sid), [])


class TestFakeYTMusic(unittest.TestCase):
    """Self-test for the FakeYTMusic stub (U5).

    FakeYTMusic mirrors the slice of ``ytmusicapi.YTMusic`` the YouTube album
    resolver service uses: ``search`` + ``get_album``. It supports per-query
    canned results, one-shot failure injection (mirroring FakeSlskdAPI), and
    call recording so service tests can assert N+1 fan-out shape.
    """

    def test_search_returns_canned_results_for_matching_query(self):
        yt = FakeYTMusic()
        canned = [{"browseId": "MPREb_abc", "title": "Test Album",
                   "artists": [{"name": "Artist"}], "year": "2020"}]
        yt.set_search("artist title", canned)

        result = yt.search("artist title", filter="albums", limit=20)

        self.assertEqual(result, canned)

    def test_search_returns_empty_list_for_unconfigured_query(self):
        yt = FakeYTMusic()

        result = yt.search("never configured", filter="albums")

        self.assertEqual(result, [])

    def test_get_album_returns_canned_response_for_matching_browse_id(self):
        yt = FakeYTMusic()
        canned = {"title": "Test Album", "audioPlaylistId": "OLAK5uy_xxx",
                  "tracks": []}
        yt.set_album("MPREb_abc", canned)

        result = yt.get_album("MPREb_abc")

        self.assertEqual(result, canned)

    def test_get_album_raises_server_error_for_unconfigured_browse_id(self):
        """Mirrors real ytmusicapi behavior: non-existent albums raise."""
        from ytmusicapi.exceptions import YTMusicServerError
        yt = FakeYTMusic()

        with self.assertRaises(YTMusicServerError):
            yt.get_album("MPREb_does_not_exist")

    def test_search_failure_injection_is_one_shot_server_error(self):
        from ytmusicapi.exceptions import YTMusicServerError
        yt = FakeYTMusic()
        yt.set_search("flaky", [{"browseId": "MPREb_z"}])
        yt.set_search_error("flaky", YTMusicServerError("upstream 503"))

        with self.assertRaises(YTMusicServerError):
            yt.search("flaky", filter="albums")
        # Second call: queued exception is gone, canned result is returned.
        self.assertEqual(
            yt.search("flaky", filter="albums"),
            [{"browseId": "MPREb_z"}],
        )

    def test_search_failure_injection_is_one_shot_user_error(self):
        from ytmusicapi.exceptions import YTMusicUserError
        yt = FakeYTMusic()
        yt.set_search_error("bad", YTMusicUserError("malformed query"))

        with self.assertRaises(YTMusicUserError):
            yt.search("bad", filter="albums")
        # Second call falls back to the empty default.
        self.assertEqual(yt.search("bad", filter="albums"), [])

    def test_search_failure_injection_is_one_shot_timeout(self):
        import requests
        yt = FakeYTMusic()
        yt.set_search_error("slow", requests.Timeout("read timed out"))

        with self.assertRaises(requests.Timeout):
            yt.search("slow", filter="albums")
        self.assertEqual(yt.search("slow", filter="albums"), [])

    def test_search_failure_injection_is_one_shot_connection_error(self):
        import requests
        yt = FakeYTMusic()
        yt.set_search_error("dropped", requests.ConnectionError("ECONNRESET"))

        with self.assertRaises(requests.ConnectionError):
            yt.search("dropped", filter="albums")
        self.assertEqual(yt.search("dropped", filter="albums"), [])

    def test_search_failure_injection_is_one_shot_key_error(self):
        """KeyError simulates ytmusicapi parser drift."""
        yt = FakeYTMusic()
        yt.set_search_error("parse_fail", KeyError("tabs"))

        with self.assertRaises(KeyError):
            yt.search("parse_fail", filter="albums")
        self.assertEqual(yt.search("parse_fail", filter="albums"), [])

    def test_get_album_failure_injection_is_one_shot_server_error(self):
        from ytmusicapi.exceptions import YTMusicServerError
        yt = FakeYTMusic()
        yt.set_album("MPREb_x", {"title": "X", "tracks": []})
        yt.set_album_error("MPREb_x", YTMusicServerError("upstream 503"))

        with self.assertRaises(YTMusicServerError):
            yt.get_album("MPREb_x")
        # Second call: canned response returns.
        self.assertEqual(yt.get_album("MPREb_x"), {"title": "X", "tracks": []})

    def test_get_album_failure_injection_is_one_shot_user_error(self):
        from ytmusicapi.exceptions import YTMusicUserError
        yt = FakeYTMusic()
        yt.set_album("MPREb_y", {"title": "Y", "tracks": []})
        yt.set_album_error("MPREb_y", YTMusicUserError("bad request"))

        with self.assertRaises(YTMusicUserError):
            yt.get_album("MPREb_y")
        self.assertEqual(yt.get_album("MPREb_y"), {"title": "Y", "tracks": []})

    def test_get_album_failure_injection_is_one_shot_timeout(self):
        import requests
        yt = FakeYTMusic()
        yt.set_album("MPREb_z", {"title": "Z", "tracks": []})
        yt.set_album_error("MPREb_z", requests.Timeout("slow"))

        with self.assertRaises(requests.Timeout):
            yt.get_album("MPREb_z")
        self.assertEqual(yt.get_album("MPREb_z"), {"title": "Z", "tracks": []})

    def test_get_album_failure_injection_is_one_shot_connection_error(self):
        import requests
        yt = FakeYTMusic()
        yt.set_album("MPREb_q", {"title": "Q", "tracks": []})
        yt.set_album_error("MPREb_q", requests.ConnectionError("ECONNRESET"))

        with self.assertRaises(requests.ConnectionError):
            yt.get_album("MPREb_q")
        self.assertEqual(yt.get_album("MPREb_q"), {"title": "Q", "tracks": []})

    def test_get_album_failure_injection_is_one_shot_key_error(self):
        yt = FakeYTMusic()
        yt.set_album("MPREb_p", {"title": "P", "tracks": []})
        yt.set_album_error("MPREb_p", KeyError("tracks"))

        with self.assertRaises(KeyError):
            yt.get_album("MPREb_p")
        self.assertEqual(yt.get_album("MPREb_p"), {"title": "P", "tracks": []})

    def test_search_records_call_arguments(self):
        yt = FakeYTMusic()

        yt.search("first query", filter="albums", limit=20)
        yt.search("second", filter=None, limit=5)

        self.assertEqual(len(yt.search_calls), 2)
        self.assertEqual(yt.search_calls[0]["query"], "first query")
        self.assertEqual(yt.search_calls[0]["filter"], "albums")
        self.assertEqual(yt.search_calls[0]["limit"], 20)
        self.assertEqual(yt.search_calls[1]["query"], "second")
        self.assertEqual(yt.search_calls[1]["filter"], None)
        self.assertEqual(yt.search_calls[1]["limit"], 5)

    def test_get_album_records_call_arguments(self):
        yt = FakeYTMusic()
        yt.set_album("MPREb_a", {"title": "A", "tracks": []})
        yt.set_album("MPREb_b", {"title": "B", "tracks": []})

        yt.get_album("MPREb_a")
        yt.get_album("MPREb_b")

        self.assertEqual(len(yt.get_album_calls), 2)
        self.assertEqual(yt.get_album_calls[0]["browseId"], "MPREb_a")
        self.assertEqual(yt.get_album_calls[1]["browseId"], "MPREb_b")

    def test_call_recording_captures_failed_calls_too(self):
        """Calls are recorded even when they raise — like FakeSlskdAPI."""
        from ytmusicapi.exceptions import YTMusicServerError
        yt = FakeYTMusic()
        yt.set_search_error("boom", YTMusicServerError("nope"))

        with self.assertRaises(YTMusicServerError):
            yt.search("boom", filter="albums")

        self.assertEqual(yt.search_calls[0]["query"], "boom")

    def test_make_album_fixture_produces_expected_top_level_shape(self):
        fixture = FakeYTMusic.make_album_fixture(
            audio_playlist_id="OLAK5uy_xxx",
            title="Test Album",
            artists=[{"name": "Artist", "id": "UCxxx"}],
            year="2020",
            tracks=[],
        )

        expected_top_keys = {
            "title", "type", "thumbnails", "description", "artists",
            "year", "trackCount", "duration", "duration_seconds",
            "audioPlaylistId", "tracks", "other_versions",
        }
        self.assertEqual(set(fixture.keys()), expected_top_keys)
        self.assertEqual(fixture["title"], "Test Album")
        self.assertEqual(fixture["audioPlaylistId"], "OLAK5uy_xxx")
        self.assertEqual(fixture["year"], "2020")
        self.assertEqual(fixture["trackCount"], 0)
        self.assertEqual(fixture["tracks"], [])
        self.assertEqual(fixture["other_versions"], [])

    def test_make_album_fixture_track_shape(self):
        track = {
            "videoId": "vid_1", "title": "Track 1",
            "artists": [{"name": "Artist", "id": "UCxxx"}],
            "album": {"name": "Test Album", "id": "MPREb_abc"},
            "duration": "3:14",
            "duration_seconds": 194,
            "trackNumber": 1,
            "isAvailable": True,
            "isExplicit": False,
            "likeStatus": "INDIFFERENT",
            "thumbnails": [],
            "feedbackTokens": {"add": None, "remove": None},
            "creditsBrowseId": None,
        }
        fixture = FakeYTMusic.make_album_fixture(
            audio_playlist_id="OLAK5uy_xxx",
            title="Test Album",
            artists=[{"name": "Artist", "id": "UCxxx"}],
            year="2020",
            tracks=[track],
        )

        expected_track_keys = {
            "videoId", "title", "artists", "album", "duration",
            "duration_seconds", "trackNumber", "isAvailable", "isExplicit",
            "likeStatus", "thumbnails", "feedbackTokens", "creditsBrowseId",
        }
        self.assertEqual(fixture["trackCount"], 1)
        self.assertEqual(set(fixture["tracks"][0].keys()), expected_track_keys)

    def test_make_album_fixture_other_versions_shape(self):
        other = {
            "browseId": "MPREb_other",
            "title": "Test Album (Deluxe)",
            "artists": [{"name": "Artist", "id": "UCxxx"}],
            "year": "2021",
            "thumbnails": [],
            "isExplicit": False,
        }
        fixture = FakeYTMusic.make_album_fixture(
            audio_playlist_id="OLAK5uy_xxx",
            title="Test Album",
            artists=[{"name": "Artist", "id": "UCxxx"}],
            year="2020",
            tracks=[],
            other_versions=[other],
        )

        expected_other_keys = {
            "browseId", "title", "artists", "year", "thumbnails", "isExplicit",
        }
        self.assertEqual(len(fixture["other_versions"]), 1)
        self.assertEqual(
            set(fixture["other_versions"][0].keys()), expected_other_keys,
        )

    def test_make_album_fixture_round_trips_through_set_album(self):
        """The fixture shape is what set_album / get_album exchange."""
        yt = FakeYTMusic()
        fixture = FakeYTMusic.make_album_fixture(
            audio_playlist_id="OLAK5uy_xxx",
            title="Test Album",
            artists=[{"name": "Artist", "id": "UCxxx"}],
            year="2020",
            tracks=[],
        )

        yt.set_album("MPREb_abc", fixture)

        self.assertEqual(yt.get_album("MPREb_abc"), fixture)


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
            imported_path="/mnt/virtio/Music/Beets/Pet Grief/Old Album",
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

    def test_imported_path_cleared_on_old_row(self):
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
        self.assertIsNone(old["imported_path"])

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

    def test_import_job_queue_methods_mirror_core_lifecycle(self):
        from lib.import_queue import IMPORT_JOB_MANUAL, manual_import_payload

        db = FakePipelineDB()
        first = db.enqueue_import_job(
            IMPORT_JOB_MANUAL,
            request_id=42,
            dedupe_key="manual:42",
            payload=manual_import_payload(failed_path="/tmp/manual"),
        )
        duplicate = db.enqueue_import_job(
            IMPORT_JOB_MANUAL,
            request_id=42,
            dedupe_key="manual:42",
            payload=manual_import_payload(failed_path="/tmp/manual"),
        )
        self.assertEqual(first.id, duplicate.id)
        self.assertTrue(duplicate.deduped)
        self.assertEqual(db.count_import_jobs_by_status(), {"queued": 1})
        db.mark_import_job_preview_importable(
            first.id,
            preview_result={"verdict": "would_import"},
            message="ready",
        )

        claimed = db.claim_next_import_job(worker_id="fake-worker")
        assert claimed is not None
        self.assertEqual(claimed.status, "running")
        self.assertEqual(claimed.attempts, 1)
        self.assertEqual(claimed.worker_id, "fake-worker")

        requeued = db.requeue_running_import_jobs(message="retry")
        self.assertEqual([job.id for job in requeued], [claimed.id])
        self.assertEqual(requeued[0].status, "queued")
        self.assertIsNone(requeued[0].worker_id)

        claimed = db.claim_next_import_job(worker_id="fake-worker-2")
        assert claimed is not None
        self.assertEqual(claimed.status, "running")
        self.assertEqual(claimed.attempts, 2)
        self.assertEqual(claimed.worker_id, "fake-worker-2")

        completed = db.mark_import_job_completed(
            claimed.id,
            result={"success": True},
            message="done",
        )
        assert completed is not None
        self.assertEqual(completed.status, "completed")

        later = db.enqueue_import_job(
            IMPORT_JOB_MANUAL,
            request_id=42,
            dedupe_key="manual:42",
            payload=manual_import_payload(failed_path="/tmp/manual"),
        )
        self.assertNotEqual(first.id, later.id)
        failed = db.mark_import_job_failed(
            later.id,
            error="boom",
            message="failed",
        )
        assert failed is not None
        self.assertEqual(failed.status, "failed")

    def test_requeue_import_job_for_preview_flips_running_back_to_waiting(self):
        from lib.import_queue import IMPORT_JOB_MANUAL, manual_import_payload

        db = FakePipelineDB()
        job = db.enqueue_import_job(
            IMPORT_JOB_MANUAL,
            request_id=42,
            dedupe_key="manual:requeue-fake",
            payload=manual_import_payload(failed_path="/tmp/manual"),
        )
        db.mark_import_job_preview_importable(
            job.id,
            preview_result={"verdict": "would_import"},
            message="ready",
        )
        claimed = db.claim_next_import_job(worker_id="importer")
        assert claimed is not None
        self.assertEqual(claimed.status, "running")
        prior_attempts = claimed.attempts
        prior_preview_attempts = claimed.preview_attempts

        updated = db.requeue_import_job_for_preview(
            claimed.id,
            reason="candidate evidence missing",
        )

        assert updated is not None
        self.assertEqual(updated.status, "queued")
        self.assertEqual(updated.preview_status, "waiting")
        self.assertIsNone(updated.worker_id)
        self.assertIsNone(updated.started_at)
        self.assertIsNone(updated.heartbeat_at)
        self.assertIsNone(updated.preview_message)
        self.assertIsNone(updated.preview_error)
        self.assertEqual(updated.message, "candidate evidence missing")
        # Counters preserved.
        self.assertEqual(updated.attempts, prior_attempts)
        self.assertEqual(updated.preview_attempts, prior_preview_attempts)

        # Now claimable by preview.
        preview = db.claim_next_import_preview_job(worker_id="preview-1")
        assert preview is not None
        self.assertEqual(preview.id, claimed.id)

    def test_requeue_import_job_for_preview_idempotent_when_not_running(self):
        from lib.import_queue import IMPORT_JOB_MANUAL, manual_import_payload

        db = FakePipelineDB()
        job = db.enqueue_import_job(
            IMPORT_JOB_MANUAL,
            request_id=42,
            dedupe_key="manual:requeue-fake-idem",
            payload=manual_import_payload(failed_path="/tmp/manual"),
        )
        # Not yet claimed by importer (preview_status='waiting', status='queued').
        result = db.requeue_import_job_for_preview(
            job.id,
            reason="not running",
        )
        self.assertIsNone(result)

    def test_import_job_queue_defaults_to_preview_waiting(self):
        from lib.import_queue import IMPORT_JOB_MANUAL, manual_import_payload

        db = FakePipelineDB()
        queued = db.enqueue_import_job(
            IMPORT_JOB_MANUAL,
            request_id=42,
            dedupe_key="manual:fresh",
            payload=manual_import_payload(failed_path="/tmp/manual"),
        )

        self.assertEqual(queued.preview_status, "waiting")
        self.assertIsNone(queued.preview_message)
        self.assertIsNone(queued.preview_completed_at)
        self.assertIsNone(queued.importable_at)
        # Preview worker can claim it; importer cannot.
        self.assertIsNone(db.claim_next_import_job(worker_id="importer"))
        claimed = db.claim_next_import_preview_job(worker_id="preview")
        assert claimed is not None
        self.assertEqual(claimed.id, queued.id)

    def test_abandon_auto_import_request_guards_state_and_logs(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            status="downloading",
            active_download_state={
                "current_path": "/tmp/staged",
                "import_subprocess_started_at": "2026-05-06T00:00:00+00:00",
            },
        ))

        log_id = db.abandon_auto_import_request(
            request_id=42,
            current_path="/tmp/staged",
            soulseek_username="alice",
            filetype="flac",
            beets_scenario="abandoned_auto_import",
            beets_detail="abandoned",
            outcome="failed",
            staged_path="/tmp/staged",
            error_message="abandoned",
            validation_result=None,
        )

        self.assertEqual(log_id, 1)
        self.assertEqual(db.request(42)["status"], "wanted")
        self.assertIsNone(db.request(42)["active_download_state"])
        self.assertEqual(db.recorded_attempts, [(42, "download")])
        self.assertEqual(
            db.download_logs[0].beets_scenario,
            "abandoned_auto_import",
        )

        second = db.abandon_auto_import_request(
            request_id=42,
            current_path="/tmp/staged",
            soulseek_username="alice",
            filetype="flac",
            beets_scenario="abandoned_auto_import",
            beets_detail="abandoned",
            outcome="failed",
            staged_path="/tmp/staged",
            error_message="abandoned",
            validation_result=None,
        )
        self.assertIsNone(second)
        self.assertEqual(len(db.download_logs), 1)

    def test_dashboard_metric_stubs_return_core_shapes(self):
        db = FakePipelineDB()

        cycle_id = db.record_cycle_metrics(cycle_total_s=12.5)
        new_peers = db.record_peer_observations(["alice", "alice", "bob"])
        repeated = db.record_peer_observations(["alice"])

        self.assertEqual(cycle_id, 1)
        self.assertEqual(db.cycle_metrics[0]["wanted_total"], 0)
        self.assertEqual(new_peers, 2)
        self.assertEqual(repeated, 0)
        peer_metrics = db.get_peer_metrics()
        self.assertEqual(peer_metrics["totals"]["known_peers"], 2)
        dashboard = db.get_pipeline_dashboard_metrics()
        self.assertIn("cycles", dashboard)
        self.assertEqual(dashboard["cycles"]["recent"][0]["cycle_total_s"],
                         12.5)
        self.assertEqual(dashboard["peers"]["totals"]["known_peers"], 2)
        self.assertEqual(
            dashboard["coverage"]["wanted_trend"]["current_wanted"], 0)

    def test_import_job_preview_methods_mirror_core_lifecycle(self):
        from lib.import_queue import IMPORT_JOB_MANUAL, manual_import_payload

        db = FakePipelineDB()
        queued = db.enqueue_import_job(
            IMPORT_JOB_MANUAL,
            request_id=42,
            dedupe_key="manual:preview",
            payload=manual_import_payload(failed_path="/tmp/manual"),
        )
        self.assertEqual(queued.preview_status, "waiting")

        claimed = db.claim_next_import_preview_job(worker_id="fake-preview")
        assert claimed is not None
        self.assertEqual(claimed.status, "queued")
        self.assertEqual(claimed.preview_status, "running")
        self.assertEqual(claimed.preview_attempts, 1)
        self.assertEqual(claimed.preview_worker_id, "fake-preview")
        self.assertTrue(db.heartbeat_import_job_preview(claimed.id))

        importable = db.mark_import_job_preview_importable(
            claimed.id,
            preview_result={"verdict": "would_import"},
            message="Preview would import",
        )
        assert importable is not None
        self.assertEqual(importable.preview_status, "evidence_ready")
        self.assertEqual(importable.preview_result, {"verdict": "would_import"})
        self.assertIsNotNone(importable.importable_at)

        rejected = db.enqueue_import_job(
            IMPORT_JOB_MANUAL,
            request_id=43,
            dedupe_key="manual:preview-reject",
            payload=manual_import_payload(failed_path="/tmp/reject"),
        )
        failed = db.mark_import_job_preview_failed(
            rejected.id,
            preview_status="confident_reject",
            error="spectral_reject",
            preview_result={
                "verdict": "confident_reject",
                "reason": "spectral_reject",
            },
            message="Preview rejected: spectral_reject",
        )
        assert failed is not None
        self.assertEqual(failed.status, "failed")
        self.assertEqual(failed.preview_status, "confident_reject")
        self.assertEqual(failed.preview_error, "spectral_reject")
        self.assertEqual(failed.error, "spectral_reject")

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
        ``imported_path``, ``*_attempts``, spectral + verified_lossless)
        so fake-backed tests don't raise ``KeyError`` where Postgres
        would return NULL/0."""
        db = FakePipelineDB()
        rid = db.add_request("X", "Y", source="request")
        row = db.request(rid)
        for key in (
            "beets_distance", "beets_scenario", "imported_path",
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

    def test_get_log_keeps_download_source_and_aliases_request_source(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, source="redownload"))
        slskd_id = db.log_download(1, outcome="success")
        yt_id = db.insert_youtube_running(
            request_id=1,
            browse_id="MPREb_fake_get_log",
            audio_playlist_id=None,
            yt_url="https://music.youtube.com/playlist?list=fake",
            expected_track_count=10,
        )
        rows = db.get_log()
        by_id = {row["id"]: row for row in rows}
        self.assertEqual(by_id[slskd_id]["source"], "slskd")
        self.assertEqual(by_id[slskd_id]["request_source"], "redownload")
        self.assertEqual(by_id[yt_id]["source"], "youtube")
        self.assertEqual(by_id[yt_id]["request_source"], "redownload")

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
        # Per-track artist defaults to None when set_tracks input omits it
        # (matches real DB: track_artist column defaults to NULL).
        self.assertEqual([r["track_artist"] for r in rows], [None, None])

    def test_set_tracks_persists_inline_track_artist(self):
        """``set_tracks`` should forward ``track_artist`` when present in
        the upstream payload (e.g. discogs adapter passes per-track
        artists directly)."""
        db = FakePipelineDB()
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
        db.set_tracks(1, [
            {"track_number": 1, "title": "T1", "track_artist": "Pre"},
        ])
        db.update_track_artists(1, [])
        self.assertEqual(
            db.get_tracks(1)[0]["track_artist"], "Pre",
        )

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

    def test_record_wrong_match_triage_merges_typed_audit(self):
        """Mirrors the real jsonb_set writer: typed audit in, omit-defaults
        dict merged onto the existing blob."""
        from lib.validation_envelope import (
            WrongMatchTriageAudit,
            decode_validation_envelope,
        )
        db = FakePipelineDB()
        db.log_download(1, outcome="rejected",
                        validation_result={"failed_path": "/p1",
                                           "scenario": "wrong_match"})
        log_id = db.download_logs[0].id
        audit = WrongMatchTriageAudit(action="deleted_reject", success=True)
        self.assertTrue(db.record_wrong_match_triage(log_id, audit))

        vr = db.download_logs[0].validation_result
        assert isinstance(vr, dict)
        # omit_defaults parity with the real writer — unset fields absent.
        self.assertEqual(vr["wrong_match_triage"],
                         {"action": "deleted_reject", "success": True})
        # Merge, not replace.
        self.assertEqual(vr["failed_path"], "/p1")
        env = decode_validation_envelope(vr)
        self.assertEqual(env.wrong_match_triage, audit)
        # Unknown log id returns False.
        self.assertFalse(db.record_wrong_match_triage(99999, audit))

    def test_clear_wrong_match_paths_clears_matching_request_and_paths(self):
        db = FakePipelineDB()
        db.log_download(1, outcome="rejected",
                        validation_result={"failed_path": "failed_imports/A",
                                           "x": 1})
        db.log_download(1, outcome="rejected",
                        validation_result={"failed_path": "/abs/A",
                                           "x": 2})
        db.log_download(1, outcome="rejected",
                        validation_result={"failed_path": "/abs/B",
                                           "x": 3})
        db.log_download(2, outcome="rejected",
                        validation_result={"failed_path": "/abs/A",
                                           "x": 4})
        db.log_download(1, outcome="success",
                        validation_result={"failed_path": "/abs/A",
                                           "x": 5})

        cleared = db.clear_wrong_match_paths(
            1, ["failed_imports/A", "/abs/A"])

        self.assertEqual(cleared, 2)
        rows = db.get_wrong_matches()
        remaining = {
            (row["request_id"], row["validation_result"]["failed_path"])  # type: ignore[index]
            for row in rows
        }
        self.assertEqual(remaining, {(1, "/abs/B"), (2, "/abs/A")})

    def test_clear_wrong_match_paths_handles_json_string_payloads(self):
        import json as _json
        db = FakePipelineDB()
        db.log_download(1, outcome="rejected",
                        validation_result=_json.dumps(
                            {"failed_path": "/p", "x": 1}))

        cleared = db.clear_wrong_match_paths(1, ["/p"])

        self.assertEqual(cleared, 1)
        stored = _json.loads(db.download_logs[0].validation_result)  # type: ignore[arg-type]
        self.assertNotIn("failed_path", stored)
        self.assertEqual(stored["x"], 1)

    def test_search_log_history(self):
        db = FakePipelineDB()
        db.log_search(1, query="a b", outcome="found", result_count=10,
                      elapsed_s=0.5)
        db.log_search(1, query="c d", outcome="no_match")

        history_1 = db.get_search_history(1)
        self.assertEqual([r["outcome"] for r in history_1],
                         ["no_match", "found"])

    def test_log_search_records_u11_forensics_kwargs(self):
        """U11 R22-R27 mirror: every new kwarg must land on the
        SearchLogRow and surface on the history dict."""
        db = FakePipelineDB()
        db.log_search(
            1, query="*adiohead Kid A", outcome="no_match",
            rejection_reason="avg_ratio_low",
            result_count_uncapped=2025,
            query_token_count=3,
            query_distinct_token_count=3,
            expected_track_count=10,
            matcher_score_top1=2.95,
            query_template="{artist} {title}",
        )
        history = db.get_search_history(1)
        self.assertEqual(len(history), 1)
        row = history[0]
        self.assertEqual(row["rejection_reason"], "avg_ratio_low")
        self.assertEqual(row["result_count_uncapped"], 2025)
        self.assertEqual(row["query_token_count"], 3)
        self.assertEqual(row["query_distinct_token_count"], 3)
        self.assertEqual(row["expected_track_count"], 10)
        score = row["matcher_score_top1"]
        assert isinstance(score, float)
        self.assertAlmostEqual(score, 2.95, places=4)
        self.assertEqual(row["query_template"], "{artist} {title}")
        # And the row dataclass preserves the raw values.
        self.assertEqual(db.search_logs[0].rejection_reason, "avg_ratio_low")
        self.assertEqual(db.search_logs[0].query_template, "{artist} {title}")

    def test_log_search_defaults_omitted_u11_kwargs_to_none(self):
        """Backwards-compat: callers that don't pass U11 kwargs get
        NULL-shaped fields on the row (mirrors the real DB column
        default for the migrated columns)."""
        db = FakePipelineDB()
        db.log_search(1, query="legacy", outcome="error")
        row = db.get_search_history(1)[0]
        self.assertIsNone(row["rejection_reason"])
        self.assertIsNone(row["result_count_uncapped"])
        self.assertIsNone(row["query_token_count"])
        self.assertIsNone(row["query_distinct_token_count"])
        self.assertIsNone(row["expected_track_count"])
        self.assertIsNone(row["matcher_score_top1"])
        self.assertIsNone(row["query_template"])

    def test_get_search_history_page_clamps_to_limit_and_seeds_cursor(self):
        """U1: cursor-paginated history mirrors PipelineDB semantics."""
        db = FakePipelineDB()
        for i in range(5):
            db.log_search(1, query=f"q{i}", outcome="no_match")
        page = db.get_search_history_page(1, limit=3)
        self.assertEqual(len(page.rows), 3)
        # Newest first.
        self.assertEqual(page.rows[0]["query"], "q4")
        self.assertEqual(page.rows[1]["query"], "q3")
        self.assertEqual(page.rows[2]["query"], "q2")
        # next_before_id seeds the next page.
        self.assertIsNotNone(page.next_before_id)

    def test_get_search_history_page_resumes_from_cursor_without_skip(self):
        db = FakePipelineDB()
        for i in range(5):
            db.log_search(1, query=f"q{i}", outcome="no_match")
        first = db.get_search_history_page(1, limit=3)
        second = db.get_search_history_page(
            1, limit=3, before_id=first.next_before_id,
        )
        self.assertEqual(len(second.rows), 2)
        self.assertEqual(second.rows[0]["query"], "q1")
        self.assertEqual(second.rows[1]["query"], "q0")
        self.assertIsNone(second.next_before_id)
        first_ids = {r["id"] for r in first.rows}
        second_ids = {r["id"] for r in second.rows}
        self.assertFalse(first_ids.intersection(second_ids))

    def test_get_search_history_page_exhausted(self):
        db = FakePipelineDB()
        db.log_search(1, query="only", outcome="no_match")
        page = db.get_search_history_page(1, limit=10)
        self.assertEqual(len(page.rows), 1)
        self.assertIsNone(page.next_before_id)

    def test_get_search_history_page_empty(self):
        db = FakePipelineDB()
        page = db.get_search_history_page(1, limit=10)
        self.assertEqual(page.rows, [])
        self.assertIsNone(page.next_before_id)

    def test_get_search_history_page_excludes_other_requests(self):
        db = FakePipelineDB()
        db.log_search(1, query="mine", outcome="no_match")
        db.log_search(2, query="theirs", outcome="no_match")
        page = db.get_search_history_page(1, limit=10)
        self.assertEqual(len(page.rows), 1)
        self.assertEqual(page.rows[0]["query"], "mine")

    def test_user_cooldowns_upsert_and_filter(self):
        db = FakePipelineDB()
        now = datetime.now(timezone.utc)
        db.add_cooldown("alice", now + timedelta(days=3), reason="x")
        db.add_cooldown("bob", now - timedelta(days=1), reason="expired")
        # Upsert — second call on alice replaces cooldown_until/reason.
        db.add_cooldown("alice", now + timedelta(days=7), reason="y")

        active = db.get_cooled_down_users()
        self.assertEqual(active, ["alice"])
        # Upsert replaced rather than duplicated alice's row.
        self.assertEqual(len(db.user_cooldowns), 2)
        self.assertEqual(db.user_cooldowns["alice"].reason, "y")

    # --- #426: recency window + search + latest-summaries mirrors ---

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

    def test_get_latest_download_summaries_mirror(self):
        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="A", album_title="T", source="request",
            mb_release_id="f-sum-1", status="wanted")
        db.log_download(rid, "old_user", "flac", "/tmp/1", outcome="rejected")
        db.log_download(rid, "new_user", "flac", "/tmp/2", outcome="success")

        summaries = db.get_latest_download_summaries([rid, 9999])
        self.assertEqual(set(summaries), {rid})
        self.assertEqual(summaries[rid]["count"], 2)
        self.assertEqual(
            summaries[rid]["latest"]["soulseek_username"], "new_user")

    # --- peer_observations roster mirror (#227) ---

    def test_peer_metrics_cumulative_totals_carry_forward(self):
        """``total_peers`` accumulates across days and carries forward
        over days with no new peers."""
        db = FakePipelineDB()
        now = datetime.now(timezone.utc)
        db.record_peer_observations(
            ["old1", "old2"], observed_at=now - timedelta(days=5))
        db.record_peer_observations(["new1"], observed_at=now)

        resp = db.get_peer_metrics(days=14)
        self.assertEqual(resp["totals"]["known_peers"], 3)
        self.assertEqual(resp["totals"]["new_24h"], 1)
        self.assertEqual(resp["totals"]["seen_24h"], 1)
        self.assertEqual(resp["days"][0]["total_peers"], 3)
        self.assertEqual(resp["days"][1]["total_peers"], 2)
        self.assertEqual(
            sum(d["new_peers"] for d in resp["days"]), 3)

    def test_peer_metrics_buckets_by_perth_local_date_not_utc(self):
        """Perth-boundary regression: ``2026-05-07 23:55 UTC`` is
        ``2026-05-08 07:55 Perth``. The fake must bucket it into
        2026-05-08, matching the real method's
        ``(first_seen_at AT TIME ZONE 'Australia/Perth')::date``
        expression."""
        db = FakePipelineDB()
        perth = ZoneInfo("Australia/Perth")
        observed_at = datetime(
            2026, 5, 7, 23, 55, tzinfo=timezone.utc,
        )
        # Sanity: the same instant in Perth-local is 2026-05-08 07:55.
        self.assertEqual(observed_at.astimezone(perth).date(),
                         date(2026, 5, 8))

        db.record_peer_observations(["alice"], observed_at=observed_at)

        with patch("tests.fakes.pipeline_db._utcnow") as fake_now:
            fake_now.return_value = datetime(
                2026, 5, 9, 5, 0, tzinfo=timezone.utc,
            )  # 2026-05-09 13:00 Perth
            resp = db.get_peer_metrics(days=14)

        by_date = {r["date"]: r for r in resp["days"]}
        self.assertEqual(by_date["2026-05-08"]["new_peers"], 1)
        self.assertEqual(by_date["2026-05-07"]["new_peers"], 0)
        self.assertEqual(by_date["2026-05-07"]["total_peers"], 0)
        self.assertEqual(by_date["2026-05-08"]["total_peers"], 1)


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


class TestFakeRecentSuccessfulUploader(unittest.TestCase):
    """Self-tests for FakePipelineDB.get_recent_successful_uploader (plan U2)."""

    def test_returns_none_on_empty_logs(self):
        db = FakePipelineDB()
        self.assertIsNone(db.get_recent_successful_uploader(42))

    def test_returns_none_when_no_successful_log(self):
        db = FakePipelineDB()
        db.log_download(42, soulseek_username="bob", outcome="rejected")
        db.log_download(42, soulseek_username="alice", outcome="failed")
        self.assertIsNone(db.get_recent_successful_uploader(42))

    def test_returns_most_recent_success(self):
        db = FakePipelineDB()
        db.log_download(42, soulseek_username="alice", outcome="success")
        db.log_download(42, soulseek_username="bob", outcome="success")
        self.assertEqual(db.get_recent_successful_uploader(42), "bob")

    def test_returns_most_recent_force_import(self):
        db = FakePipelineDB()
        db.log_download(42, soulseek_username="alice", outcome="success")
        db.log_download(42, soulseek_username="harco", outcome="force_import")
        self.assertEqual(db.get_recent_successful_uploader(42), "harco")

    def test_ignores_other_request_ids(self):
        db = FakePipelineDB()
        db.log_download(42, soulseek_username="alice", outcome="success")
        db.log_download(99, soulseek_username="bob", outcome="success")
        self.assertEqual(db.get_recent_successful_uploader(42), "alice")
        self.assertEqual(db.get_recent_successful_uploader(99), "bob")

    def test_skips_null_uploader_rows(self):
        db = FakePipelineDB()
        db.log_download(42, soulseek_username="alice", outcome="success")
        db.log_download(42, soulseek_username=None, outcome="success")
        self.assertEqual(db.get_recent_successful_uploader(42), "alice")


class TestFakeActiveImportJobForRequest(unittest.TestCase):
    """Self-tests for FakePipelineDB.get_active_import_job_for_request (plan U2)."""

    def _enqueue(self, db: FakePipelineDB, *, request_id: int, dedupe_key: str):
        from lib.import_queue import IMPORT_JOB_MANUAL, manual_import_payload
        return db.enqueue_import_job(
            IMPORT_JOB_MANUAL,
            request_id=request_id,
            dedupe_key=dedupe_key,
            payload=manual_import_payload(failed_path="/tmp/x"),
        )

    def test_returns_none_when_no_jobs(self):
        db = FakePipelineDB()
        self.assertIsNone(db.get_active_import_job_for_request(42))

    def test_returns_queued_job_for_request(self):
        db = FakePipelineDB()
        job = self._enqueue(db, request_id=42, dedupe_key="manual:42")
        result = db.get_active_import_job_for_request(42)
        assert result is not None
        self.assertEqual(result["id"], job.id)
        self.assertEqual(result["status"], "queued")

    def test_returns_running_job_for_request(self):
        db = FakePipelineDB()
        self._enqueue(db, request_id=42, dedupe_key="manual:42")
        # Move it to "would_import" then claim → status='running'.
        db.mark_import_job_preview_importable(
            db._import_jobs[0]["id"],
            preview_result={"verdict": "would_import"},
            message="ok",
        )
        claimed = db.claim_next_import_job(worker_id="w")
        assert claimed is not None
        result = db.get_active_import_job_for_request(42)
        assert result is not None
        self.assertEqual(result["status"], "running")
        self.assertEqual(result["id"], claimed.id)

    def test_returns_none_for_completed_job(self):
        db = FakePipelineDB()
        job = self._enqueue(db, request_id=42, dedupe_key="manual:42")
        db.mark_import_job_preview_importable(
            job.id,
            preview_result={"verdict": "would_import"},
            message="ok",
        )
        claimed = db.claim_next_import_job(worker_id="w")
        assert claimed is not None
        db.mark_import_job_completed(claimed.id, result={"ok": True})
        self.assertIsNone(db.get_active_import_job_for_request(42))

    def test_returns_none_for_failed_job(self):
        db = FakePipelineDB()
        job = self._enqueue(db, request_id=42, dedupe_key="manual:42")
        db.mark_import_job_preview_importable(
            job.id,
            preview_result={"verdict": "would_import"},
            message="ok",
        )
        claimed = db.claim_next_import_job(worker_id="w")
        assert claimed is not None
        db.mark_import_job_failed(claimed.id, error="boom")
        self.assertIsNone(db.get_active_import_job_for_request(42))

    def test_returns_only_jobs_for_the_requested_request_id(self):
        db = FakePipelineDB()
        self._enqueue(db, request_id=42, dedupe_key="manual:42")
        self._enqueue(db, request_id=99, dedupe_key="manual:99")
        r42 = db.get_active_import_job_for_request(42)
        r99 = db.get_active_import_job_for_request(99)
        assert r42 is not None and r99 is not None
        self.assertEqual(r42["request_id"], 42)
        self.assertEqual(r99["request_id"], 99)

    def test_returns_most_recent_job_when_multiple_active(self):
        db = FakePipelineDB()
        # First job with one dedupe_key
        first = self._enqueue(db, request_id=42, dedupe_key="manual:42:a")
        second = self._enqueue(db, request_id=42, dedupe_key="manual:42:b")
        result = db.get_active_import_job_for_request(42)
        assert result is not None
        # Most recent by id
        self.assertEqual(result["id"], max(first.id, second.id))


class TestFakeActiveImportJobsForWrongMatch(unittest.TestCase):
    def test_matches_by_download_log_path_or_source_dir(self):
        from lib.import_queue import IMPORT_JOB_FORCE, force_import_payload

        db = FakePipelineDB()
        by_log = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=1,
            payload=force_import_payload(download_log_id=10, failed_path="/other"),
        )
        by_request = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            payload=force_import_payload(download_log_id=11, failed_path="/other"),
        )
        by_path = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=2,
            payload=force_import_payload(download_log_id=12, failed_path="/failed/a"),
        )
        by_dir = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=3,
            payload=force_import_payload(
                download_log_id=13,
                failed_path="/other",
                source_dirs=["alice\\Album"],
            ),
        )
        ignored = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            payload=force_import_payload(download_log_id=14, failed_path="/failed/a"),
        )
        completed = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            payload=force_import_payload(download_log_id=15, failed_path="/failed/a"),
        )
        db.mark_import_job_preview_importable(
            completed.id,
            preview_result={"verdict": "would_import"},
            message="ok",
        )
        claimed = db.claim_next_import_job(worker_id="w")
        assert claimed is not None
        db.mark_import_job_completed(claimed.id, result={"ok": True})

        rows = db.list_active_import_jobs_for_wrong_match(
            download_log_id=10,
            request_id=42,
            failed_paths=["/failed/a"],
            source_dirs=["alice\\Album"],
            ignore_import_job_id=ignored.id,
        )

        self.assertEqual(
            {job.id for job in rows},
            {by_log.id, by_path.id, by_dir.id},
        )


def _public_methods(cls: type) -> set[str]:
    """Return the set of non-underscore method names provided by ``cls``,
    including those contributed by base classes / mixins.

    ``PipelineDB`` is composed from cluster mixins under ``lib/pipeline_db/``
    (#379), so its public API lives on the mixins, not in ``vars(PipelineDB)``.
    Walk the MRO (skipping ``object``) to recover the full surface — for a
    flat class like ``FakePipelineDB`` this is identical to ``vars(cls)``."""
    names: set[str] = set()
    for klass in cls.__mro__:
        if klass is object:
            continue
        for name, obj in vars(klass).items():
            if callable(obj) and not name.startswith("_"):
                names.add(name)
    return names


class TestPipelineDBFakeContract(unittest.TestCase):
    """Enforce FakePipelineDB stays in lockstep with PipelineDB.

    Models ``TestRouteContractAudit`` (tests/web/test_route_audit.py): the
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

    def test_fake_only_methods_stay_on_the_allowlist(self) -> None:
        """Methods on ``FakePipelineDB`` that don't mirror ``PipelineDB``
        must be intentional test helpers on an explicit allowlist.

        Catches typos in new stub names
        (``update_importred_path_by_release_id`` would pass the
        ``real - fake`` check because the method isn't on real, but
        the sigcheck never exercises it). Without this inverse
        enforcement, a typo'd stub would compile and tests against it
        would crash with ``AttributeError`` — the exact silent-drift
        vector this contract is meant to prevent.
        """
        allowed_fake_only = {
            "seed_request",
            "request",
            "assert_log",
            "set_advisory_lock_result",
            "set_cooldown_result",
            "set_update_download_state_error",
            "queue_execute_results",
            "seed_youtube_album_mapping",
        }
        real = _public_methods(PipelineDB)
        fake = _public_methods(FakePipelineDB)
        unexpected = fake - real - allowed_fake_only
        self.assertEqual(
            unexpected, set(),
            f"FakePipelineDB has methods not on PipelineDB and not on "
            f"the allowlist: {sorted(unexpected)}. If these are "
            "intentional test helpers, add them to "
            "``allowed_fake_only``. If they're typo'd stubs meant to "
            "mirror a real method, rename them.",
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


_POSITIONAL_KINDS = (
    inspect.Parameter.POSITIONAL_ONLY,
    inspect.Parameter.POSITIONAL_OR_KEYWORD,
)


def _diff_signatures(real_cls: type, fake_cls: type) -> list[str]:
    """Return a list of signature drift messages between two classes.

    The invariant the reviewers kept circling: the fake must be
    substitutable for the real in every production-valid call pattern.
    Checks, in order, what a caller could observe:

    1. Positional layout must match exactly. Any reorder, insertion,
       or rename at a positional slot would bind ``add_request("A",
       "B", "request")`` to the wrong parameter on the fake (codex R4).
    2. Every named real parameter must be declared by name on the
       fake. ``**kwargs`` absorption is NOT sufficient — a fake that
       absorbs a renamed kwarg silently reproduces the drift this
       contract is meant to prevent (round 1).
    3. Kinds must match exactly. Narrowing positional-or-keyword to
       keyword-only breaks positional callers (codex R3).
    4. Requiredness drift in both directions: real required → fake
       optional lets the fake accept calls real would reject; real
       optional → fake required crashes calls real would handle
       (codex R3).
    5. ``*args`` / ``**kwargs`` on real require equivalents on fake
       so variadic callers don't silently lose arguments.

    The fake may add trailing keyword-only parameters with defaults
    (for test-only bookkeeping) and absorb test-only extras with
    ``**kwargs`` — those are not visible to any real-valid caller so
    they do not need to be mirrored back onto real.
    """
    real_methods = _public_methods(real_cls)
    fake_methods = _public_methods(fake_cls)
    shared = real_methods & fake_methods

    mismatches: list[str] = []
    for name in sorted(shared):
        real_sig = inspect.signature(getattr(real_cls, name))
        fake_sig = inspect.signature(getattr(fake_cls, name))

        mismatches.extend(_diff_positional_layout(name, real_sig, fake_sig))
        mismatches.extend(_diff_named_params(name, real_sig, fake_sig))
        mismatches.extend(_diff_variadic(name, real_sig, fake_sig))
        mismatches.extend(_diff_fake_only_required(name, real_sig, fake_sig))
    return mismatches


def _positional_params(
    sig: inspect.Signature,
) -> list[inspect.Parameter]:
    return [
        p for p in sig.parameters.values()
        if p.name != "self" and p.kind in _POSITIONAL_KINDS
    ]


def _diff_positional_layout(
    method: str,
    real_sig: inspect.Signature,
    fake_sig: inspect.Signature,
) -> list[str]:
    """Positional slots must match real exactly — no reorder, no extras.

    Python binds positional args by index; a fake that adds
    ``add_request(album_title, artist_name, source)`` would satisfy the
    name-matching check while binding ``add_request("Artist", "Album",
    "request")`` to the wrong parameters (codex R4).
    """
    out: list[str] = []
    real_pos = _positional_params(real_sig)
    fake_pos = _positional_params(fake_sig)

    for i, rp in enumerate(real_pos):
        if i >= len(fake_pos):
            out.append(
                f"{method}: positional slot {i} ('{rp.name}') "
                "present on real but missing from fake's positional "
                "sequence")
            continue
        fp = fake_pos[i]
        if fp.name != rp.name:
            out.append(
                f"{method}: positional slot {i} — real='{rp.name}', "
                f"fake='{fp.name}' (reorder, rename, or inserted "
                "parameter would break positional callers)")
    if len(fake_pos) > len(real_pos):
        extras = [fp.name for fp in fake_pos[len(real_pos):]]
        out.append(
            f"{method}: fake has extra positional parameters beyond "
            f"real: {extras} (a positional call on real would bind "
            "nothing to these slots on the fake)")
    return out


def _diff_named_params(
    method: str,
    real_sig: inspect.Signature,
    fake_sig: inspect.Signature,
) -> list[str]:
    """Every named real param must be declared on the fake with a
    compatible kind and requiredness."""
    out: list[str] = []
    fake_params = fake_sig.parameters
    for pname, param in real_sig.parameters.items():
        if pname == "self":
            continue
        if param.kind in (inspect.Parameter.VAR_POSITIONAL,
                          inspect.Parameter.VAR_KEYWORD):
            continue
        if pname not in fake_params:
            out.append(
                f"{method}: param '{pname}' present on real but "
                "not declared on fake (declare it explicitly — "
                "**kwargs does not count)")
            continue
        fp = fake_params[pname]
        if fp.kind != param.kind:
            out.append(
                f"{method}({pname}): kind mismatch — "
                f"real={param.kind.name}, fake={fp.kind.name}")
            continue
        real_required = param.default is inspect.Parameter.empty
        fake_required = fp.default is inspect.Parameter.empty
        if real_required and not fake_required:
            out.append(
                f"{method}({pname}): real requires this param but "
                "fake gives it a default (silently makes it optional)")
        elif fake_required and not real_required:
            out.append(
                f"{method}({pname}): real has a default but fake "
                "requires this param (production calls that omit it "
                "would crash against the fake)")
    return out


def _diff_fake_only_required(
    method: str,
    real_sig: inspect.Signature,
    fake_sig: inspect.Signature,
) -> list[str]:
    """Fake params absent from real must have defaults.

    A fake that adds a required keyword-only parameter
    (e.g. ``def m(self, request_id, *, new_required):``) has no match
    in ``_diff_named_params`` — that helper walks only real params.
    Every production call that omits the new kwarg works against real
    but raises ``TypeError`` against the fake. Codex R5.

    Optional extras (with defaults) are fine — they represent
    test-only bookkeeping the fake may accept.
    """
    out: list[str] = []
    real_names = {p.name for p in real_sig.parameters.values()}
    for fp in fake_sig.parameters.values():
        if fp.name == "self":
            continue
        if fp.kind in (inspect.Parameter.VAR_POSITIONAL,
                       inspect.Parameter.VAR_KEYWORD):
            continue
        if fp.name in real_names:
            continue
        # Fake-only parameter. Required → crashes real-valid callers.
        if fp.default is inspect.Parameter.empty:
            out.append(
                f"{method}({fp.name}): fake requires a parameter not "
                "on real — production calls that omit it would crash "
                "against the fake (give it a default, or remove it)")
    return out


def _diff_variadic(
    method: str,
    real_sig: inspect.Signature,
    fake_sig: inspect.Signature,
) -> list[str]:
    """``*args`` / ``**kwargs`` on real require equivalents on fake."""
    out: list[str] = []
    fake_accepts_varargs = any(
        p.kind == inspect.Parameter.VAR_POSITIONAL
        for p in fake_sig.parameters.values())
    fake_accepts_kwargs = any(
        p.kind == inspect.Parameter.VAR_KEYWORD
        for p in fake_sig.parameters.values())
    for param in real_sig.parameters.values():
        if (param.kind == inspect.Parameter.VAR_POSITIONAL
                and not fake_accepts_varargs):
            out.append(
                f"{method}: real has *{param.name} but fake does "
                "not accept variable positional args")
        elif (param.kind == inspect.Parameter.VAR_KEYWORD
                and not fake_accepts_kwargs):
            out.append(
                f"{method}: real has **{param.name} but fake does "
                "not accept variable keyword args")
    return out


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

    def test_positional_or_keyword_narrowed_to_keyword_only_is_caught(self):
        """Codex R3: a fake that narrows pos-or-keyword to keyword-only
        would break every caller using positional args — must fail the
        contract so fake-backed tests cannot silently green."""
        class Real:
            def m(self, artist_name: str, album_title: str) -> None:
                ...
        class Fake:
            def m(self, *, artist_name: str, album_title: str) -> None:
                ...
        diff = _diff_signatures(Real, Fake)
        self.assertTrue(
            any("kind mismatch" in m for m in diff),
            f"Expected drift for narrowed kind, got: {diff}")

    def test_optional_becoming_required_on_fake_is_caught(self):
        """Codex R3: a fake that drops a default would force production
        callers to pass the arg — production calls that omit it would
        work against real but crash the fake."""
        class Real:
            def m(self, flag: bool = False) -> None:
                ...
        class Fake:
            def m(self, flag: bool) -> None:  # no default
                ...
        diff = _diff_signatures(Real, Fake)
        self.assertTrue(
            any("fake requires this param" in m for m in diff),
            f"Expected drift for tightened requiredness, got: {diff}")

    def test_positional_reorder_is_caught(self):
        """Codex R4: a fake that swaps positional parameter order
        would bind positional args to the wrong params. Name-matching
        alone cannot catch this — the positional layout must be
        checked by index."""
        class Real:
            def m(self, artist_name: str, album_title: str,
                  source: str) -> None:
                ...
        class Fake:
            def m(self, album_title: str, artist_name: str,
                  source: str) -> None:
                ...
        diff = _diff_signatures(Real, Fake)
        self.assertTrue(
            any("positional slot" in m for m in diff),
            f"Expected drift for reordered positional params, got: "
            f"{diff}")

    def test_fake_with_extra_positional_param_is_caught(self):
        """Codex R4: a fake that adds an extra positional parameter
        beyond real breaks positional callers — real's call pattern
        would leave that slot unbound on the fake."""
        class Real:
            def m(self, artist_name: str, album_title: str) -> None:
                ...
        class Fake:
            def m(self, artist_name: str, album_title: str,
                  new_required: str) -> None:
                ...
        diff = _diff_signatures(Real, Fake)
        self.assertTrue(
            any("extra positional parameters" in m for m in diff),
            f"Expected drift for fake with extra positional, got: "
            f"{diff}")

    def test_fake_with_required_keyword_only_not_on_real_is_caught(self):
        """Codex R5: a fake that adds a required keyword-only
        parameter real doesn't have would crash any production-valid
        call that omits it."""
        class Real:
            def m(self, request_id: int) -> None:
                ...
        class Fake:
            def m(self, request_id: int, *, new_required: str) -> None:
                ...
        diff = _diff_signatures(Real, Fake)
        self.assertTrue(
            any("new_required" in m and "not on real" in m
                for m in diff),
            f"Expected drift for required fake-only kwarg, got: "
            f"{diff}")

    def test_fake_with_optional_keyword_only_not_on_real_is_allowed(self):
        """Optional fake-only params (for test-only bookkeeping) are
        permitted — real-valid callers never pass them, so they don't
        affect call compatibility."""
        class Real:
            def m(self, request_id: int) -> None:
                ...
        class Fake:
            def m(self, request_id: int, *,
                  test_only: bool = False) -> None:
                ...
        self.assertEqual(_diff_signatures(Real, Fake), [])


class TestFakeBeetsDB(unittest.TestCase):
    """Self-tests for FakeBeetsDB — the minimal in-memory BeetsDB stand-in."""

    def test_check_mbids_detail_returns_seeded_rows_only(self) -> None:
        beets = FakeBeetsDB()
        beets.set_mbid_detail("mbid-1", {"beets_tracks": 11})
        out = beets.check_mbids_detail(["mbid-1", "mbid-2"])
        self.assertEqual(out, {"mbid-1": {"beets_tracks": 11}})
        self.assertEqual(beets.check_mbids_detail_calls,
                         [["mbid-1", "mbid-2"]])

    def test_get_albums_by_artist_returns_seeded_rows(self) -> None:
        beets = FakeBeetsDB()
        beets.set_albums_by_artist("X", [{"album": "A"}])
        self.assertEqual(beets.get_albums_by_artist("X", "mb-1"),
                         [{"album": "A"}])
        self.assertEqual(beets.get_albums_by_artist("Y"), [])
        self.assertEqual(beets.get_albums_by_artist_calls,
                         [("X", "mb-1"), ("Y", "")])

    def test_get_tracks_by_mb_release_id_returns_seeded_or_none(self) -> None:
        # Real method returns None when locate finds no exact hit —
        # NOT an empty list (the browse route branches on that).
        beets = FakeBeetsDB()
        tracks = [{"title": "T1", "track": 1, "disc": 1, "length": 180,
                   "format": "MP3", "bitrate": 320000,
                   "samplerate": 44100, "bitdepth": 16}]
        beets.set_tracks_for_release("mbid-1", tracks)
        self.assertEqual(beets.get_tracks_by_mb_release_id("mbid-1"), tracks)
        self.assertIsNone(beets.get_tracks_by_mb_release_id("mbid-2"))
        self.assertEqual(beets.get_tracks_by_mb_release_id_calls,
                         ["mbid-1", "mbid-2"])

    def test_get_tracks_empty_list_when_album_present_without_seeds(self) -> None:
        # Production: an exact album hit always yields a list (its
        # items), never None. 'Album present but tracks None' is not a
        # reachable state, so the fake must not express it either.
        beets = FakeBeetsDB()
        beets.set_album_ids_for_release("mbid-1", [7])
        self.assertEqual(beets.get_tracks_by_mb_release_id("mbid-1"), [])

    def test_album_id_seeds_imply_presence(self) -> None:
        # Production derives presence and album-id mapping from one
        # seam (issue #121) — seeded ids mean the release IS in
        # library. An explicit set_album_exists seed still wins.
        beets = FakeBeetsDB()
        beets.set_album_ids_for_release("mbid-1", [7])
        self.assertTrue(beets.album_exists("mbid-1"))
        self.assertEqual(beets.check_mbids(["mbid-1", "mbid-2"]), {"mbid-1"})
        beets.set_album_exists("mbid-1", False)
        self.assertFalse(beets.album_exists("mbid-1"))

    def test_get_album_ids_by_mbids_normalizes_like_production(self) -> None:
        # _batch_lookup_album_ids normalizes every input and keys the
        # result by the canonical form — '0012856590' hits the row
        # stored '12856590'.
        beets = FakeBeetsDB()
        beets.set_album_ids_for_release("12856590", [8])
        out = beets.get_album_ids_by_mbids(["0012856590"])
        self.assertEqual(out, {"12856590": 8})

    def test_get_album_ids_by_mbids_honors_album_ids_default(self) -> None:
        # The shared store's _default affordance applies to both
        # readers — get_all_album_ids_for_release and this map.
        beets = FakeBeetsDB()
        beets._album_ids_default = [5]
        self.assertEqual(beets.get_album_ids_by_mbids(["mbid-x"]),
                         {"mbid-x": 5})

    def test_search_albums_substring_matches_artist_or_album(self) -> None:
        # Real query: LIKE %q% COLLATE NOCASE on albumartist OR album,
        # ORDER BY albumartist, year, album, LIMIT.
        beets = FakeBeetsDB()
        beets.set_library_albums([
            {"id": 2, "album": "Zeta", "artist": "B Artist", "year": 2020,
             "added": 10.0},
            {"id": 1, "album": "Alpha", "artist": "A Artist", "year": 2020,
             "added": 20.0},
            {"id": 3, "album": "Unrelated", "artist": "Nobody", "year": 2020,
             "added": 30.0},
        ])
        out = beets.search_albums("aRtIsT")
        self.assertEqual([a["id"] for a in out], [1, 2])
        self.assertEqual(beets.search_albums("artist", limit=1)[0]["id"], 1)
        self.assertEqual(beets.search_albums_calls,
                         [("aRtIsT", 100), ("artist", 1)])

    def test_get_recent_sorts_by_added_desc(self) -> None:
        beets = FakeBeetsDB()
        beets.set_library_albums([
            {"id": 1, "album": "Old", "artist": "X", "added": 10.0},
            {"id": 2, "album": "New", "artist": "X", "added": 30.0},
            {"id": 3, "album": "Never stamped", "artist": "X", "added": None},
        ])
        out = beets.get_recent(limit=2)
        self.assertEqual([a["id"] for a in out], [2, 1])
        # NULL added sorts last under DESC (SQLite ordering).
        self.assertEqual(
            [a["id"] for a in beets.get_recent(limit=3)], [2, 1, 3])
        self.assertEqual(beets.get_recent_calls, [2, 3])

    def test_locate_state_derived_from_album_id_seeds(self) -> None:
        from lib.beets_db import ReleaseLocation

        beets = FakeBeetsDB()
        beets.set_album_ids_for_release(
            "11111111-1111-1111-1111-111111111111", [4])
        beets.set_album_ids_for_release("12856590", [9])
        loc = beets.locate("11111111-1111-1111-1111-111111111111")
        self.assertEqual(loc, ReleaseLocation(
            kind="exact", album_id=4,
            selectors=("mb_albumid:11111111-1111-1111-1111-111111111111",)))
        # Discogs numeric shape → both selector columns, normalized id.
        loc = beets.locate("0012856590")
        self.assertEqual(loc, ReleaseLocation(
            kind="exact", album_id=9,
            selectors=("discogs_albumid:12856590",
                       "mb_albumid:12856590")))
        self.assertEqual(
            beets.locate("unseeded-mbid"),
            ReleaseLocation(kind="absent", album_id=None, selectors=()))
        self.assertEqual(
            beets.locate_calls,
            ["11111111-1111-1111-1111-111111111111", "0012856590",
             "unseeded-mbid"])

    def test_locate_queue_consumes_in_order_and_repeats_last(self) -> None:
        from lib.beets_db import ReleaseLocation

        beets = FakeBeetsDB()
        beets.queue_locate_results([
            ReleaseLocation(kind="exact", album_id=1, selectors=()),
            ReleaseLocation(kind="absent", album_id=None, selectors=()),
        ])
        first = beets.locate("mbid-x")
        # Empty selectors on an exact entry auto-fill from the queried
        # id's shape at call time.
        self.assertEqual(first.kind, "exact")
        self.assertEqual(first.selectors, ("mb_albumid:mbid-x",))
        self.assertEqual(beets.locate("mbid-x").kind, "absent")
        self.assertEqual(beets.locate("mbid-x").kind, "absent")

    def test_get_min_bitrate_seeded_and_default(self) -> None:
        beets = FakeBeetsDB()
        beets._album_exists_default = True  # presence gate (see below)
        beets.set_min_bitrate("mbid-1", 245)
        self.assertEqual(beets.get_min_bitrate("mbid-1"), 245)
        beets._min_bitrate_default = 320
        self.assertEqual(beets.get_min_bitrate("mbid-2"), 320)
        self.assertEqual(beets.get_min_bitrate_calls,
                         ["mbid-1", "mbid-2"])

    def test_get_min_bitrate_gates_on_presence_like_production(self) -> None:
        # Production resolves presence via locate first — an absent
        # release returns None no matter what; bitrate keys normalize.
        from lib.beets_db import ReleaseLocation

        beets = FakeBeetsDB()
        beets._min_bitrate_default = 320
        self.assertIsNone(beets.get_min_bitrate("mbid-absent"))
        beets.set_album_ids_for_release("12856590", [7])
        beets.set_min_bitrate("12856590", 245)
        self.assertEqual(beets.get_min_bitrate("0012856590"), 245)
        # Queued locate head models "current" state — after a queued
        # removal lands at absent, min_bitrate goes None with it.
        beets.queue_locate_results([
            ReleaseLocation(kind="absent", album_id=None, selectors=())])
        self.assertIsNone(beets.get_min_bitrate("0012856590"))
        self.assertFalse(beets.album_exists("0012856590"))

    def test_locate_queue_rejects_impossible_locations(self) -> None:
        from lib.beets_db import ReleaseLocation

        beets = FakeBeetsDB()
        with self.assertRaises(AssertionError):
            beets.queue_locate_results([ReleaseLocation(
                kind="exact", album_id=None, selectors=())])
        with self.assertRaises(AssertionError):
            beets.queue_locate_results([ReleaseLocation(
                kind="absent", album_id=None,
                selectors=("mb_albumid:x",))])

    def test_locate_queue_passes_explicit_selectors_verbatim(self) -> None:
        from lib.beets_db import ReleaseLocation

        beets = FakeBeetsDB()
        entry = ReleaseLocation(
            kind="exact", album_id=3,
            selectors=("discogs_albumid:9", "mb_albumid:9"))
        beets.queue_locate_results([entry])
        self.assertEqual(beets.locate("9"), entry)

    def test_get_album_detail_keyed_by_album_id(self) -> None:
        beets = FakeBeetsDB()
        beets.set_album_detail(7, {"id": 7, "album": "A", "tracks": []})
        detail = beets.get_album_detail(7)
        assert detail is not None
        self.assertEqual(detail["album"], "A")
        detail["album"] = "mutated"
        got = beets.get_album_detail(7)
        assert got is not None
        self.assertEqual(got["album"], "A")
        self.assertIsNone(beets.get_album_detail(8))
        self.assertEqual(beets.get_album_detail_calls, [7, 7, 8])

    def test_get_album_ids_by_mbids_derives_from_release_id_seeds(self) -> None:
        # Shares the set_album_ids_for_release seed store so presence
        # and album-id mapping can't disagree (the paired-consistency
        # concern from issue #121 the real _batch_lookup_album_ids
        # exists to solve). Exact hit → first seeded album id.
        beets = FakeBeetsDB()
        beets.set_album_ids_for_release("mbid-1", [17, 18])
        beets.set_album_ids_for_release("mbid-empty", [])
        out = beets.get_album_ids_by_mbids(["mbid-1", "mbid-empty", "mbid-2"])
        self.assertEqual(out, {"mbid-1": 17})
        self.assertEqual(beets.get_album_ids_by_mbids_calls,
                         [["mbid-1", "mbid-empty", "mbid-2"]])

    def test_album_exists_returns_seeded_value(self) -> None:
        beets = FakeBeetsDB()
        beets.set_album_exists("mbid-1", True)
        beets.set_album_exists("mbid-2", False)
        self.assertTrue(beets.album_exists("mbid-1"))
        self.assertFalse(beets.album_exists("mbid-2"))
        # Unseeded keys default to False (matches "no row" semantics).
        self.assertFalse(beets.album_exists("mbid-unknown"))
        self.assertEqual(
            beets.album_exists_calls,
            ["mbid-1", "mbid-2", "mbid-unknown"],
        )

    def test_get_album_info_keyed_by_release_id(self) -> None:
        from lib.beets_db import AlbumInfo
        beets = FakeBeetsDB()
        info = AlbumInfo(
            album_id=7,
            track_count=10,
            min_bitrate_kbps=320,
            avg_bitrate_kbps=320,
            median_bitrate_kbps=320,
            format="MP3",
            is_cbr=False,
            album_path="/Beets/Artist/Album",
        )
        beets.set_album_info("mbid-1", info)
        # Two-arg form (matches real signature: mb_release_id + cfg).
        self.assertIs(beets.get_album_info("mbid-1", None), info)
        # Unseeded returns None.
        self.assertIsNone(beets.get_album_info("mbid-unknown"))
        self.assertEqual(
            beets.get_album_info_calls,
            ["mbid-1", "mbid-unknown"],
        )

    def test_check_mbids_uses_seeded_album_exists_state(self) -> None:
        beets = FakeBeetsDB()
        beets.set_album_exists("mbid-1", True)
        beets.set_album_exists("missing", False)

        self.assertEqual(beets.check_mbids(["mbid-1", "missing"]), {"mbid-1"})
        self.assertEqual(beets.check_mbids_calls, [["mbid-1", "missing"]])

    def test_list_release_identities_returns_seeded_rows(self) -> None:
        beets = FakeBeetsDB()
        beets.set_release_identities([
            {
                "id": 7,
                "album": "Album",
                "albumartist": "Artist",
                "mb_albumid": "mbid-1",
                "discogs_albumid": None,
            },
        ])

        rows = beets.list_release_identities()

        self.assertEqual(rows[0]["mb_albumid"], "mbid-1")
        self.assertEqual(beets.list_release_identities_calls, 1)

    def test_get_all_album_ids_for_release_returns_list(self) -> None:
        beets = FakeBeetsDB()
        beets.set_album_ids_for_release("mbid-1", [77, 88])
        self.assertEqual(beets.get_all_album_ids_for_release("mbid-1"), [77, 88])
        # Unseeded returns empty list (matches "no row" semantics).
        self.assertEqual(beets.get_all_album_ids_for_release("mbid-other"), [])

    def test_get_item_paths_returns_list_of_pairs(self) -> None:
        beets = FakeBeetsDB()
        paths = [(11, "/Beets/01.flac"), (12, "/Beets/02.flac")]
        beets.set_item_paths("mbid-1", paths)
        self.assertEqual(beets.get_item_paths("mbid-1"), paths)
        self.assertEqual(beets.get_item_paths("mbid-other"), [])

    def test_close_is_context_manager(self) -> None:
        beets = FakeBeetsDB()
        with beets as ctx:
            self.assertIs(ctx, beets)
            self.assertEqual(beets.close_calls, 0)
        self.assertEqual(beets.close_calls, 1)


class TestFakePipelineDBUnfindable(unittest.TestCase):
    """Self-tests for U13 ``FakePipelineDB`` unfindable-detection writers.

    Mirrors ``.claude/rules/code-quality.md`` § "Every new PipelineDB
    method needs an equivalent stub on ``FakePipelineDB`` with a self-
    test in ``tests/test_fakes.py``." Each test exercises a single
    fake method's contract — call recording + persisted row state.
    """

    def test_record_artist_probe_writes_and_records(self) -> None:
        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="A", album_title="B", source="request",
            mb_release_id="m-uf-1",
        )
        ts = datetime(2026, 5, 26, tzinfo=timezone.utc)
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
        ts = datetime(2026, 5, 26, tzinfo=timezone.utc)
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
        now = datetime.now(timezone.utc)
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
            ts = datetime(2026, 5, 20, tzinfo=timezone.utc)
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
        original_rescue_at = datetime(2026, 1, 15, tzinfo=timezone.utc)
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
        meta = entry["youtube_metadata"]
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
            with self.subTest(outcome=bogus):
                with self.assertRaises(ValueError):
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

    def test_list_active_youtube_rescues_returns_request_context(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42, artist_name="YT Artist", album_title="YT Album",
            mb_release_id="yt-mbid", status="wanted",
        ))
        yt_id = db.insert_youtube_running(**self._payload(
            42, browse_id="MPREb_visible",
        ))

        rows = db.list_active_youtube_rescues(limit=10)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["download_log_id"], yt_id)
        self.assertEqual(rows[0]["request_id"], 42)
        self.assertEqual(rows[0]["artist_name"], "YT Artist")
        self.assertEqual(rows[0]["album_title"], "YT Album")
        self.assertEqual(rows[0]["request_status"], "wanted")
        self.assertEqual(
            rows[0]["youtube_metadata"]["browse_id"], "MPREb_visible")

        db.update_youtube_terminal(
            yt_id, "youtube_failed", {"reason": "operator_cancelled"},
        )
        self.assertEqual(db.list_active_youtube_rescues(limit=10), [])

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


class TestFakeDashboardMirror(unittest.TestCase):
    """The dashboard read-model mirror aggregates real seeded telemetry
    and must emit a fully JSON-serializable envelope (production
    isoformats every timestamp at the _isoformat_or_none boundary —
    a raw datetime here 500s the dashboard route)."""

    def _seeded_db(self) -> FakePipelineDB:
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="wanted"))
        db.record_cycle_metrics(
            cycle_total_s=300.0, search_time_s=240.0, peers_browsed=8,
            find_download_queued=4, find_download_completed=4,
            wanted_total=10,
        )
        db.log_search(
            1, query="q", outcome="found", result_count=5, elapsed_s=2.0,
            variant="v1", final_state="Completed", browse_time_s=42.0,
            peers_browsed=110, peers_browsed_lazy=5, fanout_waves=6,
        )
        db.log_search(1, query="q2", outcome="no_match", elapsed_s=1.0)
        db.record_peer_observations(["peer-a", "peer-b"])
        return db

    def test_envelope_is_json_serializable_with_seeded_telemetry(self):
        import json
        db = self._seeded_db()
        payload = db.get_pipeline_dashboard_metrics()
        json.dumps(payload)  # raises TypeError on any leaked datetime

    def test_windows_and_coverage_aggregate_seeded_rows(self):
        db = self._seeded_db()
        payload = db.get_pipeline_dashboard_metrics()
        win24 = payload["searches"]["windows"][0]
        self.assertEqual(win24["label"], "24h")
        self.assertEqual(win24["searches"], 2)
        self.assertEqual(win24["outcomes"]["found"], 1)
        self.assertEqual(win24["outcomes"]["no_match"], 1)
        cov = payload["coverage"]
        self.assertEqual(cov["matches_24h"], 1)
        self.assertEqual(cov["wanted_total"], 1)
        self.assertEqual(cov["wanted_searched_24h"], 1)
        # Production zero-fills the series via generate_series — DENSE:
        # always exactly 24 hourly / 28 daily buckets.
        self.assertEqual(len(cov["match_rate_series_24h"]), 24)
        self.assertEqual(len(cov["match_rate_series_28d"]), 28)
        self.assertEqual(
            sum(pt["matches"] for pt in cov["match_rate_series_24h"]), 1)
        # Heavy-query panel surfaces the browse-heavy row.
        heavy = payload["peers"]["heavy_queries"]
        self.assertEqual(len(heavy), 1)
        self.assertEqual(heavy[0]["peers_browsed"], 110)
        self.assertEqual(heavy[0]["peer_dirs"], 115)
        cyc24 = payload["cycles"]["windows"][0]
        self.assertEqual(cyc24["cycles"], 1)
        self.assertEqual(cyc24["find_download_queued"], 4)

    def test_empty_db_emits_complete_envelope(self):
        import json
        payload = FakePipelineDB().get_pipeline_dashboard_metrics()
        json.dumps(payload)
        self.assertEqual(payload["searches"]["windows"][0]["searches"], 0)
        self.assertEqual(payload["coverage"]["wanted_total"], 0)
        self.assertEqual(payload["peers"]["heavy_queries"], [])
        # Dense zero-filled series even with zero telemetry.
        self.assertEqual(
            len(payload["coverage"]["match_rate_series_24h"]), 24)
        self.assertEqual(
            len(payload["coverage"]["match_rate_series_28d"]), 28)
        # Never null — production emits 0 when there are no searches.
        self.assertEqual(payload["coverage"]["top_10_share_24h"], 0)

    def test_cycle_rows_use_production_serializer_keys(self):
        """recent/outliers rows carry the renamed watchdog_kills key and
        NOT the raw cycle_metrics column names production never emits."""
        db = self._seeded_db()
        payload = db.get_pipeline_dashboard_metrics()
        recent = payload["cycles"]["recent"]
        self.assertEqual(len(recent), 1)
        row = recent[0]
        self.assertIn("watchdog_kills", row)
        self.assertNotIn("cycle_searches_watchdog_killed", row)
        self.assertNotIn("cache_pos_hits", row)
        self.assertNotIn("wanted_total", row)
        self.assertIsInstance(row["created_at"], str)

    def test_exhausted_outcome_counts_as_reset_in_suspects(self):
        """Production's reset_24h counts the HISTORICAL ``exhausted``
        outcome; problem_24h is restricted to timeout/error/empty_query."""
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="wanted"))
        db.log_search(1, query="q", outcome="exhausted")
        db.log_search(1, query="q", outcome="timeout")
        db.log_search(1, query="q", outcome="some_unknown_outcome")
        payload = db.get_pipeline_dashboard_metrics()
        suspects = payload["coverage"]["top_loop_suspects"]
        self.assertEqual(len(suspects), 1)
        self.assertEqual(suspects[0]["reset_24h"], 1)
        self.assertEqual(suspects[0]["problem_24h"], 1)
        # Search-window errors bucket mirrors the SQL FILTER: the
        # unknown outcome counts toward searches but NO bucket.
        win24 = payload["searches"]["windows"][0]
        self.assertEqual(win24["searches"], 3)
        self.assertEqual(win24["outcomes"]["errors"], 1)
        self.assertEqual(win24["outcomes"]["exhausted"], 1)

    def test_stale_wanted_includes_recently_searched_and_caps_at_12(self):
        """Production's stale panel is the 12 oldest-searched backlog
        rows ordered last_search_at ASC NULLS FIRST — recently-searched
        rows are included, never-searched rows sort first."""
        db = FakePipelineDB()
        for rid in range(1, 15):
            db.seed_request(make_request_row(id=rid, status="wanted"))
        db.log_search(1, query="q", outcome="no_match")  # searched 1h ago
        payload = db.get_pipeline_dashboard_metrics()
        stale = payload["coverage"]["stale_wanted"]
        self.assertEqual(len(stale), 12)
        # Never-searched rows lead; the searched row sorts last and IS
        # eligible (it would be excluded only by the LIMIT, with 14
        # backlog rows it falls off the end).
        self.assertIsNone(stale[0]["last_search_at"])
        self.assertNotIn(1, [r["request_id"] for r in stale])

    def test_heavy_queries_lazy_only_rows_qualify(self):
        """The filter is (peers_browsed + peers_browsed_lazy) > 0 — a
        lazy-only browse row qualifies; result_count coerces to int."""
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="wanted"))
        db.log_search(1, query="lazy", outcome="no_match",
                      peers_browsed_lazy=7)
        payload = db.get_pipeline_dashboard_metrics()
        heavy = payload["peers"]["heavy_queries"]
        self.assertEqual(len(heavy), 1)
        self.assertEqual(heavy[0]["peer_dirs"], 7)
        self.assertEqual(heavy[0]["result_count"], 0)


class TestFakeCursor(unittest.TestCase):
    """FakeCursor pairs with FakePipelineDB.queue_execute_results for
    raw-SQL seams (web.overlay.check_pipeline et al.). Consumption
    semantics mirror real psycopg2 cursors (test-fidelity Rule B)."""

    def test_fetchall_returns_rows(self):
        rows = [{"id": 1}, {"id": 2}]
        self.assertEqual(FakeCursor(rows).fetchall(), rows)

    def test_fetchone_consumes_like_a_real_cursor(self):
        cur = FakeCursor([{"id": 1}, {"id": 2}])
        self.assertEqual(cur.fetchone(), {"id": 1})
        self.assertEqual(cur.fetchone(), {"id": 2})
        self.assertIsNone(cur.fetchone())
        self.assertIsNone(FakeCursor().fetchone())

    def test_fetchall_after_fetchone_returns_remainder(self):
        cur = FakeCursor([{"id": 1}, {"id": 2}, {"id": 3}])
        cur.fetchone()
        self.assertEqual(cur.fetchall(), [{"id": 2}, {"id": 3}])
        self.assertEqual(cur.fetchall(), [])

    def test_while_fetchone_loop_terminates(self):
        cur = FakeCursor([{"id": 1}, {"id": 2}])
        drained = []
        while (row := cur.fetchone()) is not None:
            drained.append(row)
        self.assertEqual(len(drained), 2)

    def test_empty_default_fetchall(self):
        self.assertEqual(FakeCursor().fetchall(), [])

    def test_queued_through_fake_pipeline_db_execute(self):
        db = FakePipelineDB()
        db.queue_execute_results(FakeCursor([{"id": 7}]))
        cur = db._execute("SELECT 1", ())
        self.assertEqual(cur.fetchall(), [{"id": 7}])

    def test_unqueued_execute_returns_empty_cursor_not_none(self):
        """Production _execute always returns a cursor; the unqueued
        fake degrades to "query ran, zero rows" instead of a None that
        AttributeErrors at the caller's fetchall()."""
        db = FakePipelineDB()
        cur = db._execute("SELECT 1", ())
        self.assertEqual(cur.fetchall(), [])
        self.assertIsNone(cur.fetchone())


class TestFakeDownloadLogCounts(unittest.TestCase):
    """State-derived mirror of PipelineDB.get_download_log_counts —
    parity with the real SQL is pinned in tests/test_pipeline_db.py."""

    def test_counts_derive_from_logged_state(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1))
        db.log_download(1, outcome="success")
        db.log_download(1, outcome="force_import")
        db.log_download(1, outcome="rejected")
        db.log_search(1, outcome="found")
        db.log_search(1, outcome="found")
        db.log_search(1, outcome="error")
        # Age one found-row out of the 6h window only, one out of both.
        db.search_logs[0].created_at -= timedelta(hours=12)
        db.log_search(1, outcome="found")
        db.search_logs[-1].created_at -= timedelta(days=2)

        counts = db.get_download_log_counts()
        self.assertEqual(counts.total, 3)
        self.assertEqual(counts.imported, 2)
        self.assertEqual(counts.matches_24h, 2)
        self.assertEqual(counts.matches_6h, 1)


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
        })

    def test_empty_mbids_short_circuits(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=7, mb_release_id="mbid-1"))
        self.assertEqual(db.get_pipeline_overlay([]), {})


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
            id=1, mb_release_id="mbid-x", status="manual"))
        self.assertEqual(db.request(1)["status"], "manual")

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

    def test_update_request_fields_rejects_duplicate_mb_release_id(self):
        # Production's UPDATE hits the same UNIQUE as the INSERT.
        import psycopg2.errors

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, mb_release_id="mbid-1"))
        db.seed_request(make_request_row(id=2, mb_release_id="mbid-2"))
        with self.assertRaises(psycopg2.errors.UniqueViolation):
            db.update_request_fields(2, mb_release_id="mbid-1")
        self.assertEqual(db.request(2)["mb_release_id"], "mbid-2")

    def test_update_request_fields_setting_own_mbid_is_a_noop(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, mb_release_id="mbid-1"))
        db.update_request_fields(1, mb_release_id="mbid-1", status="manual")
        self.assertEqual(db.request(1)["status"], "manual")


class TestFakeDownloadLogIdMint(unittest.TestCase):
    """Minted download_log ids mirror production's sequence-backed PK.

    A test that rewinds ``_next_download_log_id`` below an existing id
    used to mint duplicates silently — the three accessors then disagree
    (oldest vs max-id vs insertion order). The mint guard makes that a
    hard error (#445 item 4; previously a local assert in
    ``test_routes_imports._seed_wrong_match``).
    """

    def test_log_download_rejects_rewound_counter_collision(self):
        import psycopg2.errors

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1))
        db.log_download(1, outcome="rejected")
        db._next_download_log_id = 0
        with self.assertRaises(psycopg2.errors.UniqueViolation):
            db.log_download(1, outcome="rejected")

    def test_log_download_rejects_regressed_id_even_without_collision(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1))
        db._next_download_log_id = 4  # pin → next id is 5
        db.log_download(1, outcome="rejected")
        db._next_download_log_id = 1  # would mint 2 — a sequence never regresses
        with self.assertRaises(AssertionError):
            db.log_download(1, outcome="rejected")

    def test_insert_youtube_running_shares_the_mint_guard(self):
        import psycopg2.errors

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="wanted"))
        db.log_download(1, outcome="rejected")
        db._next_download_log_id = 0
        with self.assertRaises(psycopg2.errors.UniqueViolation):
            db.insert_youtube_running(
                request_id=1, browse_id="b", audio_playlist_id=None,
                yt_url="u", expected_track_count=10)

    def test_forward_pinning_still_works(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1))
        db.log_download(1, outcome="rejected")
        db._next_download_log_id = 41  # forward pin — ids stay monotonic
        self.assertEqual(db.log_download(1, outcome="rejected"), 42)



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


class TestFakePipelineDBSearchLedger(unittest.TestCase):
    """Self-tests for the slskd search-id write-ahead ledger stubs
    (migration 044, issue #576)."""

    def test_record_search_id_appears_in_unswept_when_old_enough(self):
        db = FakePipelineDB()
        db.record_search_id("sid-1", "plan_search", 42)
        rows = db.get_unswept_search_ids(
            older_than=datetime.now(timezone.utc) + timedelta(seconds=1))
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["search_id"], "sid-1")
        self.assertEqual(rows[0]["purpose"], "plan_search")
        self.assertEqual(rows[0]["request_id"], 42)

    def test_get_unswept_search_ids_respects_older_than_cutoff(self):
        # A row created "now" is not yet older than a cutoff in the past —
        # mirrors the sweep's GRACE window (in-flight searches of the
        # current cycle are excluded).
        db = FakePipelineDB()
        db.record_search_id("sid-1", "plan_search", 1)
        rows = db.get_unswept_search_ids(
            older_than=datetime.now(timezone.utc) - timedelta(hours=1))
        self.assertEqual(rows, [])

    def test_record_search_id_is_idempotent_on_conflict(self):
        # ON CONFLICT DO NOTHING: re-recording the same id is a call-
        # recording event, but the table state (and its created_at) is
        # NOT overwritten by the second call.
        db = FakePipelineDB()
        db.record_search_id("sid-1", "plan_search", 1)
        first = db._search_ledger["sid-1"].created_at
        db.record_search_id("sid-1", "artist_probe", 2)
        self.assertEqual(db._search_ledger["sid-1"].created_at, first)
        self.assertEqual(db._search_ledger["sid-1"].purpose, "plan_search")
        self.assertEqual(len(db.record_search_id_calls), 2)

    def test_mark_search_ids_deleted_removes_from_unswept(self):
        db = FakePipelineDB()
        db.record_search_id("sid-1", "plan_search", 1)
        db.record_search_id("sid-2", "plan_search", 2)
        db.mark_search_ids_deleted(["sid-1"])
        rows = db.get_unswept_search_ids(
            older_than=datetime.now(timezone.utc) + timedelta(seconds=1))
        self.assertEqual([r["search_id"] for r in rows], ["sid-2"])

    def test_mark_search_ids_deleted_unknown_id_is_a_noop(self):
        db = FakePipelineDB()
        db.mark_search_ids_deleted(["never-recorded"])  # must not raise

    def test_prune_search_ledger_removes_only_old_deleted_rows(self):
        db = FakePipelineDB()
        db.record_search_id("sid-old", "plan_search", 1)
        db.record_search_id("sid-recent", "plan_search", 2)
        db.record_search_id("sid-undeleted", "plan_search", 3)
        db.mark_search_ids_deleted(["sid-old", "sid-recent"])
        db._search_ledger["sid-old"].deleted_at = (
            datetime.now(timezone.utc) - timedelta(days=10))

        removed = db.prune_search_ledger(
            deleted_before=datetime.now(timezone.utc) - timedelta(days=7))

        self.assertEqual(removed, 1)
        self.assertNotIn("sid-old", db._search_ledger)
        self.assertIn("sid-recent", db._search_ledger)
        self.assertIn("sid-undeleted", db._search_ledger)


class TestFakePipelineDBTransferLedger(unittest.TestCase):
    """Self-tests for the slskd transfer write-ahead ownership ledger
    stubs (migration 045, issue #571)."""

    def test_record_transfer_enqueue_appears_in_owned_transfers(self):
        db = FakePipelineDB()
        db.record_transfer_enqueue([
            TransferLedgerRow(
                request_id=42, username="peer0", filename="Music\\a.flac",
                attempt_fingerprint="abcd1234"),
        ])
        rows = db.get_owned_transfers(request_id=42)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["request_id"], 42)
        self.assertEqual(rows[0]["username"], "peer0")
        self.assertEqual(rows[0]["filename"], "Music\\a.flac")
        self.assertEqual(rows[0]["attempt_fingerprint"], "abcd1234")
        self.assertIsNone(rows[0]["local_path"])
        self.assertIsNone(rows[0]["completed_at"])
        self.assertEqual(len(db.record_transfer_enqueue_calls), 1)

    def test_record_transfer_enqueue_empty_list_is_a_noop(self):
        db = FakePipelineDB()
        db.record_transfer_enqueue([])  # must not raise
        self.assertEqual(db.get_owned_transfers(), [])

    def test_record_transfer_enqueue_writes_one_row_per_file(self):
        db = FakePipelineDB()
        db.record_transfer_enqueue([
            TransferLedgerRow(request_id=1, username="p0", filename="a.flac"),
            TransferLedgerRow(request_id=1, username="p0", filename="b.flac"),
        ])
        self.assertEqual(len(db.get_owned_transfers(request_id=1)), 2)

    def test_stamp_transfer_completion_stamps_matching_row(self):
        db = FakePipelineDB()
        db.record_transfer_enqueue([
            TransferLedgerRow(request_id=1, username="p0", filename="a.flac"),
        ])
        completed_at = datetime.now(timezone.utc)
        stamped = db.stamp_transfer_completion(
            "p0", "a.flac", "/downloads/complete/a.flac", completed_at)
        self.assertEqual(stamped, 1)
        row = db.get_owned_transfers(request_id=1)[0]
        self.assertEqual(row["local_path"], "/downloads/complete/a.flac")
        self.assertEqual(row["completed_at"], completed_at)

    def test_stamp_transfer_completion_unledgered_pair_is_a_noop(self):
        db = FakePipelineDB()
        stamped = db.stamp_transfer_completion(
            "foreign-peer", "foreign.flac", "/downloads/x",
            datetime.now(timezone.utc))
        self.assertEqual(stamped, 0)
        self.assertEqual(db.get_owned_local_paths(), set())

    def test_stamp_transfer_completion_prefers_newest_open_row(self):
        # Two retries for the same (username, filename): only the newest
        # not-yet-stamped row gets the completion stamp.
        db = FakePipelineDB()
        db.record_transfer_enqueue([
            TransferLedgerRow(request_id=1, username="p0", filename="a.flac"),
        ])
        old_id = next(iter(db._transfer_ledger))
        db._transfer_ledger[old_id].enqueued_at = (
            datetime.now(timezone.utc) - timedelta(minutes=10))
        db.record_transfer_enqueue([
            TransferLedgerRow(request_id=1, username="p0", filename="a.flac"),
        ])
        completed_at = datetime.now(timezone.utc)
        db.stamp_transfer_completion(
            "p0", "a.flac", "/downloads/complete/a.flac", completed_at)
        rows = db.get_owned_transfers(request_id=1)
        stamped_rows = [r for r in rows if r["completed_at"] is not None]
        self.assertEqual(len(stamped_rows), 1)
        self.assertNotEqual(stamped_rows[0]["id"], old_id)

    def test_get_owned_local_paths_only_returns_stamped_rows(self):
        db = FakePipelineDB()
        db.record_transfer_enqueue([
            TransferLedgerRow(request_id=1, username="p0", filename="a.flac"),
            TransferLedgerRow(request_id=1, username="p0", filename="b.flac"),
        ])
        db.stamp_transfer_completion(
            "p0", "a.flac", "/downloads/a.flac", datetime.now(timezone.utc))
        self.assertEqual(db.get_owned_local_paths(), {"/downloads/a.flac"})

    def test_get_owned_transfer_keys_empty_before_any_record(self):
        self.assertEqual(FakePipelineDB().get_owned_transfer_keys(), set())

    def test_get_owned_transfer_keys_reflects_all_rows_stamped_or_not(self):
        # Membership, not completion state: stamped and unstamped rows
        # both contribute, and duplicate retries collapse into one key.
        db = FakePipelineDB()
        db.record_transfer_enqueue([
            TransferLedgerRow(request_id=1, username="p0", filename="a.flac"),
            TransferLedgerRow(request_id=1, username="p0", filename="a.flac"),
            TransferLedgerRow(request_id=2, username="p1", filename="b.flac"),
        ])
        db.stamp_transfer_completion(
            "p0", "a.flac", "/downloads/a.flac", datetime.now(timezone.utc))
        self.assertEqual(
            db.get_owned_transfer_keys(),
            {("p0", "a.flac"), ("p1", "b.flac")})

    def test_prune_transfer_ledger_keeps_active_request_rows(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="downloading"))
        db.record_transfer_enqueue([
            TransferLedgerRow(request_id=1, username="p0", filename="a.flac"),
        ])
        old_id = next(iter(db._transfer_ledger))
        db._transfer_ledger[old_id].enqueued_at = (
            datetime.now(timezone.utc) - timedelta(days=200))

        removed = db.prune_transfer_ledger(
            older_than=datetime.now(timezone.utc) - timedelta(days=90))

        self.assertEqual(removed, 0)
        self.assertIn(old_id, db._transfer_ledger)

    def test_prune_transfer_ledger_removes_old_terminal_rows(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="imported"))
        db.record_transfer_enqueue([
            TransferLedgerRow(request_id=1, username="p0", filename="a.flac"),
        ])
        old_id = next(iter(db._transfer_ledger))
        db._transfer_ledger[old_id].enqueued_at = (
            datetime.now(timezone.utc) - timedelta(days=200))

        removed = db.prune_transfer_ledger(
            older_than=datetime.now(timezone.utc) - timedelta(days=90))

        self.assertEqual(removed, 1)
        self.assertNotIn(old_id, db._transfer_ledger)

    def test_prune_transfer_ledger_keeps_rows_inside_retention(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="imported"))
        db.record_transfer_enqueue([
            TransferLedgerRow(request_id=1, username="p0", filename="a.flac"),
        ])

        removed = db.prune_transfer_ledger(
            older_than=datetime.now(timezone.utc) - timedelta(days=90))

        self.assertEqual(removed, 0)

    def test_prune_transfer_ledger_treats_missing_request_as_inactive(self):
        # A request_id whose row no longer exists (hard-deleted elsewhere)
        # can never come back to wanted/downloading -- prunable.
        db = FakePipelineDB()
        db.record_transfer_enqueue([
            TransferLedgerRow(request_id=999, username="p0", filename="a.flac"),
        ])
        old_id = next(iter(db._transfer_ledger))
        db._transfer_ledger[old_id].enqueued_at = (
            datetime.now(timezone.utc) - timedelta(days=200))

        removed = db.prune_transfer_ledger(
            older_than=datetime.now(timezone.utc) - timedelta(days=90))

        self.assertEqual(removed, 1)


class TestFakeSlskdEvents(unittest.TestCase):
    """Self-tests for the events sub-API fake (issue #146)."""

    def _api(self):
        from tests.fakes import FakeSlskdAPI
        return FakeSlskdAPI()

    def test_pagination_slices_newest_first_feed(self):
        api = self._api()
        events = [
            api.events.make_event(
                id=f"ev-{i}", timestamp="2026-07-01T00:00:00.0000000Z",
                type="Noise", data="{}")
            for i in range(5)
        ]
        api.events.set_events(events)

        page = api.events.list(limit=2, offset=1)

        self.assertEqual([e.id for e in page.events], ["ev-1", "ev-2"])
        self.assertEqual(page.total_count, 5)
        self.assertEqual(api.events.list_calls, [(2, 1)])

    def test_total_count_override(self):
        api = self._api()
        api.events.total_count_override = 389110

        page = api.events.list()

        self.assertEqual(page.total_count, 389110)
        self.assertEqual(page.events, [])

    def test_list_error_injection(self):
        api = self._api()
        api.events.list_error = RuntimeError("events API down")

        with self.assertRaises(RuntimeError):
            api.events.list()

    def test_call_log_records_cross_api_ordering(self):
        api = self._api()

        api.transfers.get_all_downloads()
        api.events.list()

        self.assertEqual(
            api.call_log, ["transfers.get_all_downloads", "events.list"])


if __name__ == "__main__":
    unittest.main()
