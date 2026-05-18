"""Tests for lightweight fakes and shared builders."""

import inspect
import unittest
from datetime import date, datetime, timedelta, timezone
from typing import Any
from unittest.mock import patch
from zoneinfo import ZoneInfo

from lib.grab_list import DownloadFile, GrabListEntry
from lib.pipeline_db import PipelineDB, RequestSpectralStateUpdate
from lib.quality import SpectralContext, SpectralMeasurement, ValidationResult
from tests.fakes import FakePipelineDB, FakeSlskdAPI
from tests.helpers import (
    make_album_quality_evidence,
    make_download_file,
    make_grab_list_entry,
    make_request_row,
    make_spectral_context,
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

    def test_set_manual_writes_reason(self):
        """U6 fake parity: ``set_manual`` flips status and writes reason."""
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="downloading"))

        db.set_manual(42, manual_reason="search_exhausted")

        row = db.request(42)
        self.assertEqual(row["status"], "manual")
        self.assertEqual(row["manual_reason"], "search_exhausted")
        self.assertIn((42, "manual"), db.status_history)

    def test_set_manual_does_not_overwrite_existing_reason_when_none(self):
        """U6 fake parity: a None reason must NOT clobber a populated reason."""
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42, status="manual", manual_reason="operator_hold"))

        db.set_manual(42)

        row = db.request(42)
        self.assertEqual(row["status"], "manual")
        self.assertEqual(row["manual_reason"], "operator_hold")

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


class TestFakeIsRequestPlanCurrent(unittest.TestCase):
    """Stale-completion guard helper used by U5 ownership/transition gates."""

    def _make_active(self, db, rid, gen="g1"):
        from lib.pipeline_db import SearchPlanItemInput
        return db.create_successful_search_plan(
            request_id=rid, generator_id=gen,
            items=[
                SearchPlanItemInput(ordinal=0, strategy="default", query="Q0"),
                SearchPlanItemInput(ordinal=1, strategy="default", query="Q1"),
            ],
        )

    def test_current_state_returns_true(self):
        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="A", album_title="B", source="request",
            mb_release_id="m1")
        plan_id = self._make_active(db, rid)
        self.assertTrue(db.is_request_plan_current(rid, plan_id, 0, 0))

    def test_different_plan_id_is_stale(self):
        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="A", album_title="B", source="request",
            mb_release_id="m1")
        self._make_active(db, rid)
        self.assertFalse(db.is_request_plan_current(rid, 999, 0, 0))

    def test_advanced_ordinal_is_stale_for_old_ordinal(self):
        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="A", album_title="B", source="request",
            mb_release_id="m1")
        plan_id = self._make_active(db, rid)
        db.update_request_fields(rid, next_plan_ordinal=1)
        self.assertFalse(db.is_request_plan_current(rid, plan_id, 0, 0))
        self.assertTrue(db.is_request_plan_current(rid, plan_id, 1, 0))

    def test_cycle_advanced_is_stale_for_old_cycle(self):
        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="A", album_title="B", source="request",
            mb_release_id="m1")
        plan_id = self._make_active(db, rid)
        db.update_request_fields(rid, plan_cycle_count=1)
        self.assertFalse(db.is_request_plan_current(rid, plan_id, 0, 0))
        self.assertTrue(db.is_request_plan_current(rid, plan_id, 0, 1))

    def test_unknown_request_is_stale(self):
        db = FakePipelineDB()
        self.assertFalse(db.is_request_plan_current(99999, 1, 0, 0))


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
        "is_request_plan_current",
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
        self.assertTrue(db.heartbeat_import_job(claimed.id))

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
        new_dirs = db.record_peer_dir_observations([
            ("alice", "Artist\\Album"),
            ("alice", "Artist\\Album"),
            ("bob", "Other\\Album"),
        ])
        repeated = db.record_peer_dir_observations([
            ("alice", "Artist\\Album"),
        ])

        self.assertEqual(cycle_id, 1)
        self.assertEqual(db.cycle_metrics[0]["wanted_total"], 0)
        self.assertEqual(new_dirs, 2)
        self.assertEqual(repeated, 0)
        peer_metrics = db.get_peer_dir_daily_metrics()
        self.assertEqual(peer_metrics["totals"]["known_combos"], 2)
        dashboard = db.get_pipeline_dashboard_metrics()
        self.assertIn("cycles", dashboard)
        self.assertEqual(dashboard["cycles"]["recent"][0]["cycle_total_s"],
                         12.5)
        self.assertEqual(dashboard["peer_dirs"]["totals"]["known_combos"], 2)
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

        rows = db.get_user_cooldowns()
        # Newest cooldown_until first.
        self.assertEqual([r["username"] for r in rows], ["alice", "bob"])
        self.assertEqual(rows[0]["reason"], "y")

    # --- U3: peer_dir_daily_aggregates lazy-fill mirror ---

    def test_peer_dir_daily_metrics_uses_seeded_cache_rows_directly(self):
        """Cache-hit path: rows seeded into the in-memory cache flow
        through to the response without re-aggregating observations."""
        db = FakePipelineDB()
        perth = ZoneInfo("Australia/Perth")
        today_perth = datetime.now(perth).date()
        yday = today_perth - timedelta(days=1)
        two_ago = today_perth - timedelta(days=2)
        # Seed cache with values that cannot have been computed from the
        # (empty) observations dict -- proves the response came from
        # cache rather than from a recompute.
        db.peer_dir_daily_aggregates[yday] = {
            "new_combos": 7, "new_peers": 3, "new_dirs": 5,
        }
        db.peer_dir_daily_aggregates[two_ago] = {
            "new_combos": 11, "new_peers": 2, "new_dirs": 4,
        }

        resp = db.get_peer_dir_daily_metrics(days=14)

        by_date = {row["date"]: row for row in resp["days"]}
        self.assertEqual(by_date[yday.isoformat()]["new_combos"], 7)
        self.assertEqual(by_date[yday.isoformat()]["new_peers"], 3)
        self.assertEqual(by_date[yday.isoformat()]["new_dirs"], 5)
        self.assertEqual(by_date[two_ago.isoformat()]["new_combos"], 11)
        # Cache was not mutated.
        self.assertEqual(
            db.peer_dir_daily_aggregates[yday],
            {"new_combos": 7, "new_peers": 3, "new_dirs": 5},
        )

    def test_peer_dir_daily_metrics_lazy_fills_then_serves_from_cache(self):
        """Lazy-fill path: empty cache + seeded observations -> first
        call computes & stores; second call reuses cache rows."""
        db = FakePipelineDB()
        perth = ZoneInfo("Australia/Perth")
        # Seed an observation that is cleanly inside yesterday's Perth
        # bucket: noon Perth yesterday.
        yday_perth = (datetime.now(perth) - timedelta(days=1)).date()
        observed_at = datetime.combine(
            yday_perth, datetime.min.time().replace(hour=12),
            tzinfo=perth,
        ).astimezone(timezone.utc)
        db.record_peer_dir_observations(
            [("alice", "/music/a"), ("bob", "/music/b")],
            observed_at=observed_at,
        )

        # Pre-condition: cache empty.
        self.assertEqual(db.peer_dir_daily_aggregates, {})

        resp1 = db.get_peer_dir_daily_metrics(days=14)

        # Cache was populated for yesterday (and every other completed
        # day in the window).
        self.assertIn(yday_perth, db.peer_dir_daily_aggregates)
        self.assertEqual(
            db.peer_dir_daily_aggregates[yday_perth],
            {"new_combos": 2, "new_peers": 2, "new_dirs": 2},
        )

        # Mutate the cached row to a sentinel; the next call must read
        # from cache (sentinel surfaces) rather than recomputing.
        db.peer_dir_daily_aggregates[yday_perth] = {
            "new_combos": 999, "new_peers": 999, "new_dirs": 999,
        }
        resp2 = db.get_peer_dir_daily_metrics(days=14)

        by_date1 = {r["date"]: r for r in resp1["days"]}
        by_date2 = {r["date"]: r for r in resp2["days"]}
        self.assertEqual(by_date1[yday_perth.isoformat()]["new_combos"], 2)
        self.assertEqual(by_date2[yday_perth.isoformat()]["new_combos"], 999)

    def test_peer_dir_daily_metrics_buckets_by_perth_local_date_not_utc(self):
        """Perth-boundary regression: ``2026-05-07 23:55 UTC`` is
        ``2026-05-08 07:55 Perth``. The fake must bucket it into
        2026-05-08, matching the real method's
        ``(first_seen_at AT TIME ZONE 'Australia/Perth')::date``
        expression. The pre-U3 fake bucketed by UTC date and would
        have placed it in 2026-05-07 instead.
        """
        db = FakePipelineDB()
        perth = ZoneInfo("Australia/Perth")
        observed_at = datetime(
            2026, 5, 7, 23, 55, tzinfo=timezone.utc,
        )
        # Sanity: the same instant in Perth-local is 2026-05-08 07:55.
        self.assertEqual(observed_at.astimezone(perth).date(),
                         date(2026, 5, 8))

        db.record_peer_dir_observations(
            [("alice", "/music/a")], observed_at=observed_at,
        )

        # Drive the method "as if" today were 2026-05-09 Perth so
        # 2026-05-08 falls into the completed-day window and gets
        # cached. We use a generous window to cover both candidate
        # buckets regardless of the real wall-clock date.
        with patch("tests.fakes._utcnow") as fake_now:
            fake_now.return_value = datetime(
                2026, 5, 9, 5, 0, tzinfo=timezone.utc,
            )  # 2026-05-09 13:00 Perth
            resp = db.get_peer_dir_daily_metrics(days=14)

        by_date = {r["date"]: r for r in resp["days"]}
        # The Perth-bucketed observation lands in 2026-05-08, not
        # 2026-05-07. UTC-bucketing (the pre-U3 behavior) would have
        # placed the count on 2026-05-07 instead.
        self.assertEqual(by_date["2026-05-08"]["new_combos"], 1)
        self.assertEqual(by_date["2026-05-07"]["new_combos"], 0)
        # Cache rows reflect the same Perth-bucketing: 2026-05-08 has
        # the observation, 2026-05-07 is a zero row.
        self.assertEqual(
            db.peer_dir_daily_aggregates[date(2026, 5, 8)],
            {"new_combos": 1, "new_peers": 1, "new_dirs": 1},
        )
        self.assertEqual(
            db.peer_dir_daily_aggregates[date(2026, 5, 7)],
            {"new_combos": 0, "new_peers": 0, "new_dirs": 0},
        )


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
        db.log_download(42, soulseek_username="alice", outcome="error")
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
