"""Tests for lightweight fakes and shared builders."""

import copy
import inspect
import unittest
from datetime import UTC, date, datetime, timedelta
from typing import Any, cast
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

import msgspec

from lib.beets_db import AlbumInfo
from lib.grab_list import DownloadFile, GrabListEntry
from lib.pipeline_db import (
    PersistedDistance,
    PersistedTrack,
    PersistedYoutubeRow,
    PipelineDB,
    RequestSpectralStateUpdate,
    TransferLedgerRow,
)
from lib.pipeline_db._shared import REQUEST_METADATA_RESERVED_FIELDS
from lib.quality import (
    AlbumQualityEvidenceFile,
    AudioQualityMeasurement,
    AudioToolDiagnostic,
    AudioValidationReport,
    SpectralMeasurement,
    ValidationResult,
    legacy_unrecorded_audio_validation_report,
)
from tests.fakes import (
    FakeBeetsDB,
    FakeCursor,
    FakePipelineDB,
    FakeSlskdAPI,
    FakeYTMusic,
    RecordingProcessAlbum,
)
from tests.helpers import (
    claim_next_import_job,
    claim_next_import_preview_job,
    handoff_automation_owner,
    make_album_quality_evidence,
    make_ctx_with_fake_db,
    make_download_file,
    make_grab_list_entry,
    make_request_row,
    make_validation_result,
)


class TestRecordingProcessAlbum(unittest.TestCase):
    def test_records_exact_call_and_returns_configured_result(self) -> None:
        from lib.download_processing import CompletionDeferred

        entry = make_grab_list_entry()
        db = FakePipelineDB()
        ctx = make_ctx_with_fake_db(db)
        outcome = CompletionDeferred(detail="release_lock_contention")
        recorder = RecordingProcessAlbum(outcome=outcome)

        result = recorder(entry, ctx, import_job_id=73)

        self.assertIs(result, outcome)
        self.assertEqual(len(recorder.calls), 1)
        call = recorder.calls[0]
        self.assertIs(call.album_data, entry)
        self.assertIs(call.ctx, ctx)
        self.assertEqual(call.import_job_id, 73)
        self.assertIsNone(call.validate_fn)
        self.assertIsNone(call.handle_valid_fn)
        self.assertIsNone(call.dispatch_fn)


class TestFakePipelineDB(unittest.TestCase):
    def test_exposes_configured_connection_identity(self) -> None:
        db = FakePipelineDB(dsn="postgresql://contract-test")

        self.assertEqual(db.dsn, "postgresql://contract-test")

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

    def test_add_denylist_ignores_duplicate_like_postgres(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42))

        db.add_denylist(42, "peer", "first")
        db.add_denylist(42, "peer", "second")

        self.assertEqual(
            db.get_denylisted_users(42),
            [{"username": "peer", "reason": "first", "created_at": None}],
        )

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

    def test_album_quality_evidence_rejects_malformed_content_key(self):
        evidence = make_album_quality_evidence(
            mb_release_id="mb-malformed-content-key",
        )
        wrong_fingerprint = (
            "0" * 64
            if evidence.snapshot_fingerprint != "0" * 64
            else "1" * 64
        )
        malformed = msgspec.structs.replace(
            evidence,
            snapshot_fingerprint=wrong_fingerprint,
        )
        db = FakePipelineDB()

        with self.assertRaisesRegex(ValueError, "snapshot_fingerprint"):
            db.upsert_album_quality_evidence(malformed)

        self.assertIsNone(
            db.find_album_quality_evidence(
                mb_release_id=malformed.mb_release_id,
                snapshot_fingerprint=wrong_fingerprint,
            )
        )

    def test_album_quality_evidence_stale_writer_preserves_spectral_pair(self):
        """issue #829 Phase 5 PR1 review round 2, should-fix 7: this guard
        (upsert_album_quality_evidence's spectral-preserve CASE) had no
        self-test at all. A stale writer with no grade must preserve the
        stored spectral_grade AND the four #829 capture fields — mirrors
        the real SQL's CASE guard in lib/pipeline_db/evidence.py."""
        from lib.quality import AudioQualityMeasurement

        db = FakePipelineDB()
        evidence = make_album_quality_evidence(
            mb_release_id="mb-stale-writer-preserve",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=192,
                format="MP3",
                spectral_grade="genuine",
                spectral_subject="source",
                spectral_provenance="measured",
                cliff_hz=16500,
                codec_family="mp3",
                ultrasonic_deficit_db=44.0,
                spectral_measurement_version=2,
            ),
        )
        db.upsert_album_quality_evidence(evidence)

        stale_writer = msgspec.structs.replace(
            evidence,
            measurement=msgspec.structs.replace(
                evidence.measurement,
                spectral_grade=None,
                spectral_bitrate_kbps=None,
                spectral_subject=None,
                spectral_provenance=None,
                cliff_hz=None,
                codec_family=None,
                ultrasonic_deficit_db=None,
                spectral_measurement_version=None,
            ),
        )
        db.upsert_album_quality_evidence(stale_writer)

        loaded = db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert loaded is not None
        self.assertEqual(loaded.measurement.spectral_grade, "genuine")
        self.assertEqual(loaded.measurement.cliff_hz, 16500)
        self.assertEqual(loaded.measurement.codec_family, "mp3")
        self.assertEqual(loaded.measurement.ultrasonic_deficit_db, 44.0)
        self.assertEqual(loaded.measurement.spectral_measurement_version, 2)

    def test_album_quality_evidence_lattice_preserve_mirrors_the_real_sql(self):
        """The fake's AAC-lattice guard must mirror
        ``upsert_album_quality_evidence``'s CASE exactly (issue #829 PR-A):
        a writer with no lattice preserves the stored one, a writer with one
        replaces it wholesale. The real-PG twins live in
        ``tests/test_pipeline_db.py::TestAlbumQualityEvidenceStorage``."""
        from lib.quality import AacLatticeCapture, AacLatticeTrackScore

        db = FakePipelineDB()
        capture = AacLatticeCapture.from_tracks([
            AacLatticeTrackScore(
                filename="01.flac", offset=960, z=28.0, proba=0.13,
            ),
        ])
        evidence = make_album_quality_evidence(
            mb_release_id="mb-lattice-preserve",
            aac_lattice=capture,
        )
        db.upsert_album_quality_evidence(evidence)
        db.upsert_album_quality_evidence(
            msgspec.structs.replace(evidence, aac_lattice=None)
        )

        loaded = db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert loaded is not None
        self.assertEqual(loaded.aac_lattice, capture)

        replacement = AacLatticeCapture.from_tracks([
            AacLatticeTrackScore(filename="01.flac", error="boom"),
        ])
        db.upsert_album_quality_evidence(
            msgspec.structs.replace(evidence, aac_lattice=replacement)
        )
        reloaded = db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert reloaded is not None
        self.assertEqual(reloaded.aac_lattice, replacement)

    def test_album_quality_evidence_rejects_an_inconsistent_lattice(self):
        """The fake enforces the same shape the migration's CHECK does,
        because both delegate to ``AacLatticeCapture.validation_errors``."""
        from lib.quality import AacLatticeCapture, AacLatticeTrackScore

        db = FakePipelineDB()
        with self.assertRaisesRegex(ValueError, "scored_tracks must count"):
            db.upsert_album_quality_evidence(make_album_quality_evidence(
                mb_release_id="mb-lattice-bad-shape",
                aac_lattice=AacLatticeCapture(
                    tracks=[AacLatticeTrackScore(
                        filename="01.flac", offset=1, z=1.0, proba=0.1,
                    )],
                    modal_offset=1, modal_count=1, scored_tracks=7, max_z=1.0,
                ),
            ))

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

    def test_get_latest_download_log_candidate_evidence_id(self):
        """Issue #813 tooling tier: replaying the request's real last
        candidate needs the newest download_log candidate_evidence_id."""
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42))
        db.seed_request(make_request_row(id=99))

        # No download attempts yet.
        self.assertIsNone(db.get_latest_download_log_candidate_evidence_id(42))

        older_log_id = db.log_download(request_id=42, outcome="rejected")
        older_evidence = make_album_quality_evidence(mb_release_id="mb-older")
        db.upsert_album_quality_evidence(older_evidence)
        older_persisted = db.find_album_quality_evidence(
            mb_release_id=older_evidence.mb_release_id,
            snapshot_fingerprint=older_evidence.snapshot_fingerprint,
        )
        assert older_persisted is not None and older_persisted.id is not None
        db.set_download_log_candidate_evidence(older_log_id, older_persisted.id)

        # A download attempt with no candidate evidence (e.g. failed before
        # measurement) must not shadow the older one, and must not win over
        # the newer evidence-bearing row added below.
        db.log_download(request_id=42, outcome="failed")

        newer_log_id = db.log_download(request_id=42, outcome="rejected")
        newer_evidence = make_album_quality_evidence(mb_release_id="mb-newer")
        db.upsert_album_quality_evidence(newer_evidence)
        newer_persisted = db.find_album_quality_evidence(
            mb_release_id=newer_evidence.mb_release_id,
            snapshot_fingerprint=newer_evidence.snapshot_fingerprint,
        )
        assert newer_persisted is not None and newer_persisted.id is not None
        db.set_download_log_candidate_evidence(newer_log_id, newer_persisted.id)

        # A row on a DIFFERENT request must never leak in.
        other_log_id = db.log_download(request_id=99, outcome="rejected")
        other_evidence = make_album_quality_evidence(mb_release_id="mb-other")
        db.upsert_album_quality_evidence(other_evidence)
        other_persisted = db.find_album_quality_evidence(
            mb_release_id=other_evidence.mb_release_id,
            snapshot_fingerprint=other_evidence.snapshot_fingerprint,
        )
        assert other_persisted is not None and other_persisted.id is not None
        db.set_download_log_candidate_evidence(other_log_id, other_persisted.id)

        self.assertEqual(
            db.get_latest_download_log_candidate_evidence_id(42),
            newer_persisted.id,
        )
        self.assertEqual(
            db.get_latest_download_log_candidate_evidence_id(99),
            other_persisted.id,
        )
        self.assertIsNone(
            db.get_latest_download_log_candidate_evidence_id(12345))

    def test_album_quality_evidence_supports_import_job_addressing(self):
        from lib.import_queue import IMPORT_JOB_FORCE

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42))
        job = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            payload={"download_log_id": 1, "failed_path": "/tmp/candidate"},
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

    def test_album_quality_evidence_v0_claim_is_once_only(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, mb_release_id="mb-claim-1"))
        evidence = make_album_quality_evidence(
            mb_release_id="mb-claim-1",
            v0_metric=None,
        )
        db.upsert_album_quality_evidence(evidence)
        persisted = db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert persisted is not None and persisted.id is not None
        db.set_request_current_evidence(42, persisted.id)

        first = db.claim_current_v0_research_attempt(
            request_id=42,
            expected_evidence_id=persisted.id,
            expected_snapshot_fingerprint=persisted.snapshot_fingerprint,
        )
        second = db.claim_current_v0_research_attempt(
            request_id=42,
            expected_evidence_id=persisted.id,
            expected_snapshot_fingerprint=persisted.snapshot_fingerprint,
        )

        self.assertTrue(first)
        self.assertFalse(second)
        claimed = db.load_album_quality_evidence_by_id(persisted.id)
        assert claimed is not None
        self.assertTrue(claimed.on_disk_v0_research_attempted)

    def test_album_quality_evidence_current_spectral_write_is_exact(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, mb_release_id="mb-spectral-1"))
        evidence = make_album_quality_evidence(
            mb_release_id="mb-spectral-1",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=320,
                avg_bitrate_kbps=320,
                median_bitrate_kbps=320,
                format="MP3",
                spectral_grade=None,
                spectral_bitrate_kbps=None,
            ),
        )
        db.upsert_album_quality_evidence(evidence)
        persisted = db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert persisted is not None and persisted.id is not None
        db.set_request_current_evidence(42, persisted.id)

        wrong_fingerprint = db.persist_current_spectral_measurement(
            request_id=42,
            expected_evidence_id=persisted.id,
            expected_snapshot_fingerprint="wrong",
            grade="genuine",
            bitrate_kbps=96,
        )
        exact = db.persist_current_spectral_measurement(
            request_id=42,
            expected_evidence_id=persisted.id,
            expected_snapshot_fingerprint=persisted.snapshot_fingerprint,
            grade="genuine",
            bitrate_kbps=96,
            cliff_hz=17500,
            codec_family="mp3",
            ultrasonic_deficit_db=12.5,
            spectral_measurement_version=2,
        )
        # Issue #815 fresh-audit-wins: a disagreeing fresh measured audit of
        # the SAME snapshot overwrites (mirrors the production SQL, which
        # dropped the fill-only-if-NULL guard). Issue #829 Phase 5 finding A
        # (round 3 review): the four capture facts travel with the grade as
        # one atomic fact through this writer too.
        overwrite = db.persist_current_spectral_measurement(
            request_id=42,
            expected_evidence_id=persisted.id,
            expected_snapshot_fingerprint=persisted.snapshot_fingerprint,
            grade="likely_transcode",
            bitrate_kbps=160,
            cliff_hz=13000,
            codec_family="aac",
            ultrasonic_deficit_db=30.0,
            spectral_measurement_version=2,
        )

        self.assertFalse(wrong_fingerprint)
        self.assertTrue(exact)
        self.assertTrue(overwrite)
        stored = db.load_album_quality_evidence_by_id(persisted.id)
        assert stored is not None
        self.assertEqual(stored.measurement.spectral_grade, "likely_transcode")
        self.assertEqual(stored.measurement.spectral_bitrate_kbps, 160)
        self.assertEqual(stored.measurement.cliff_hz, 13000)
        self.assertEqual(stored.measurement.codec_family, "aac")
        self.assertEqual(stored.measurement.ultrasonic_deficit_db, 30.0)
        self.assertEqual(stored.measurement.spectral_measurement_version, 2)
        self.assertEqual(stored.measurement.spectral_subject, "installed")
        self.assertEqual(stored.measurement.spectral_provenance, "measured")

    def test_album_quality_evidence_attempt_marker_is_monotonic(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42))
        evidence = make_album_quality_evidence(
            mb_release_id="mb-monotonic-1",
            v0_metric=None,
            on_disk_v0_research_attempted=True,
        )
        db.upsert_album_quality_evidence(evidence)

        db.upsert_album_quality_evidence(msgspec.structs.replace(
            evidence,
            on_disk_v0_research_attempted=False,
        ))

        persisted = db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert persisted is not None
        self.assertTrue(persisted.on_disk_v0_research_attempted)

    def test_album_quality_evidence_enrichment_gate_is_monotonic(self):
        db = FakePipelineDB()
        evidence = make_album_quality_evidence(
            mb_release_id="mb-enrichment-gate-1",
            current_enrichment_required=True,
        )
        db.upsert_album_quality_evidence(evidence)

        db.upsert_album_quality_evidence(msgspec.structs.replace(
            evidence,
            current_enrichment_required=False,
        ))

        persisted = db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert persisted is not None
        self.assertTrue(persisted.current_enrichment_required)

    def test_album_quality_evidence_absent_v0_preserves_stored_tuple(self):
        from lib.quality import AlbumQualityV0Metric

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42))
        metric = AlbumQualityV0Metric(
            min_bitrate_kbps=201,
            avg_bitrate_kbps=259,
            median_bitrate_kbps=255,
            subject="installed",
            provenance="measured",
        )
        evidence = make_album_quality_evidence(
            mb_release_id="mb-preserve-v0-1",
            v0_metric=metric,
            on_disk_v0_research_attempted=True,
        )
        db.upsert_album_quality_evidence(evidence)

        db.upsert_album_quality_evidence(msgspec.structs.replace(
            evidence,
            v0_metric=None,
            on_disk_v0_research_attempted=False,
        ))

        persisted = db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert persisted is not None
        self.assertEqual(persisted.v0_metric, metric)
        self.assertTrue(persisted.on_disk_v0_research_attempted)

        replacement = AlbumQualityV0Metric(
            avg_bitrate_kbps=261,
            subject="installed",
        )
        db.upsert_album_quality_evidence(msgspec.structs.replace(
            evidence,
            v0_metric=replacement,
            on_disk_v0_research_attempted=False,
        ))
        replaced = db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert replaced is not None
        self.assertEqual(replaced.v0_metric, replacement)
        self.assertTrue(replaced.on_disk_v0_research_attempted)

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
            audio_corrupt=True,
            audio_error=(
                "01 - Track.mp3: Invalid data found when processing input"
            ),
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
        self.assertEqual(
            loaded.audio_error,
            "01 - Track.mp3: Invalid data found when processing input",
        )
        self.assertEqual(loaded.folder_layout, "nested")
        self.assertEqual(loaded.audio_file_count, 1)
        self.assertEqual(loaded.filetype_band, "mp3")
        self.assertEqual(loaded.matched_bad_audio_hash_id, 99)
        self.assertEqual(loaded.matched_bad_audio_hash_path, "01 - Track.mp3")
        self.assertFalse(loaded.files[0].decode_ok)

    def test_weak_writer_preserves_strong_audio_validation_tuple(self):
        """A stale fake writer cannot erase a completed decoder audit."""
        db = FakePipelineDB()
        files = [
            AlbumQualityEvidenceFile(
                relative_path="disc-1/01.flac",
                size_bytes=123,
                mtime_ns=456,
                extension="flac",
                container="flac",
                codec="flac",
                decode_ok=False,
            ),
        ]
        report = AudioValidationReport(
            tool_version="8.1.1",
            outcome="audio_corrupt",
            files_checked=1,
            files_failed=1,
            diagnostics=[
                AudioToolDiagnostic(
                    relative_path="disc-1/01.flac",
                    category="decode_error",
                    return_code=69,
                    stderr_excerpt="Invalid data",
                    stderr_bytes=4096,
                    stderr_sha256="a" * 64,
                    stderr_truncated=True,
                ),
            ],
        )
        strong = make_album_quality_evidence(
            mb_release_id="mb-audio-audit-fake",
            files=files,
            audio_corrupt=True,
            audio_error="disc-1/01.flac: Invalid data",
            audio_validation=report,
        )
        db.upsert_album_quality_evidence(strong)
        db.upsert_album_quality_evidence(msgspec.structs.replace(
            strong,
            audio_validation=legacy_unrecorded_audio_validation_report(),
            audio_corrupt=False,
            audio_error=None,
            files=[msgspec.structs.replace(files[0], decode_ok=True)],
        ))

        loaded = db.find_album_quality_evidence(
            mb_release_id=strong.mb_release_id,
            snapshot_fingerprint=strong.snapshot_fingerprint,
        )
        assert loaded is not None
        self.assertEqual(loaded.audio_validation, report)
        self.assertTrue(loaded.audio_corrupt)
        self.assertEqual(loaded.audio_error, strong.audio_error)
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

    def test_read_only_query_cursor_brackets_query_with_setup_and_rollback(self):
        db = FakePipelineDB()
        query_cursor = FakeCursor([{"id": 1}])
        db.queue_execute_results(
            MagicMock(name="begin"), MagicMock(name="string_mode"),
            query_cursor, MagicMock(name="rollback"),
        )

        with db.read_only_query_cursor() as cursor:
            cursor.execute("SELECT 1")
            self.assertEqual(cursor.fetchall(), [{"id": 1}])

        self.assertEqual(
            db.execute_calls,
            [
                ("BEGIN TRANSACTION READ ONLY", ()),
                ("SET LOCAL standard_conforming_strings = on", ()),
                ("SELECT 1", ()),
                ("ROLLBACK", ()),
            ],
        )

    def test_read_only_query_cursor_rolls_back_after_query_error(self):
        db = FakePipelineDB()
        error = RuntimeError("query failed")
        db.queue_execute_results(
            MagicMock(name="begin"), MagicMock(name="string_mode"), error,
            MagicMock(name="rollback"),
        )

        with self.assertRaisesRegex(RuntimeError, "query failed"), db.read_only_query_cursor() as cursor:
            cursor.execute("SELECT broken")

        self.assertEqual(
            db.execute_calls,
            [
                ("BEGIN TRANSACTION READ ONLY", ()),
                ("SET LOCAL standard_conforming_strings = on", ()),
                ("SELECT broken", ()),
                ("ROLLBACK", ()),
            ],
        )

    def test_read_only_query_cursor_suppresses_connection_lost_during_cleanup(self):
        import psycopg2

        db = FakePipelineDB()
        query_cursor = FakeCursor([{"id": 1}])
        db.queue_execute_results(
            MagicMock(name="begin"), MagicMock(name="string_mode"),
            query_cursor, psycopg2.InterfaceError("connection lost"),
        )

        with db.read_only_query_cursor() as cursor:
            cursor.execute("SELECT 1")
            rows = cursor.fetchall()

        self.assertEqual(rows, [{"id": 1}])
        self.assertEqual(db.execute_calls[-1], ("ROLLBACK", ()))

    def test_read_only_query_cursor_propagates_non_connection_cleanup_error(self):
        db = FakePipelineDB()
        db.queue_execute_results(
            MagicMock(name="begin"), MagicMock(name="string_mode"),
            FakeCursor(), RuntimeError("rollback failed"),
        )

        with self.assertRaisesRegex(RuntimeError, "rollback failed"), db.read_only_query_cursor() as cursor:
            cursor.execute("SELECT 1")

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

    def test_dashboard_wanted_total_includes_downloading_and_processing(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="wanted"))
        db.seed_request(make_request_row(id=2, status="downloading"))
        db.seed_request(make_request_row(id=3, status="imported"))
        processing_id = db.add_request(
            "Artist",
            "Processing",
            "request",
            mb_release_id="fake-dashboard-processing",
        )
        handoff_automation_owner(db, processing_id)

        db.record_cycle_metrics(cycle_total_s=1.0)
        dashboard = db.get_pipeline_dashboard_metrics()

        self.assertEqual(db.cycle_metrics[0]["wanted_total"], 3)
        self.assertEqual(
            dashboard["coverage"]["wanted_trend"]["current_wanted"], 3)

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

    def test_empty_spectral_adapter_cannot_report_missing_or_replaced_success(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="replaced"))
        before = copy.deepcopy(db.request(42))
        empty = RequestSpectralStateUpdate()

        self.assertFalse(db.update_spectral_state(42, empty))
        self.assertFalse(db.update_spectral_state(999, empty))
        self.assertEqual(db.request(42), before)

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

    def test_get_downloading(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="downloading"))
        db.seed_request(make_request_row(id=2, status="wanted"))
        db.seed_request(make_request_row(id=3, status="downloading"))

        rows = db.get_downloading()
        self.assertEqual(len(rows), 2)
        ids = {r["id"] for r in rows}
        self.assertEqual(ids, {1, 3})

    def test_get_retained_failure_paths(self):
        db = FakePipelineDB()
        db.log_download(
            request_id=1,
            outcome="measurement_failed",
            staged_path="/downloads/retained",
        )
        db.log_download(
            request_id=1,
            outcome="measurement_failed",
            staged_path="",
        )
        db.log_download(
            request_id=1,
            outcome="rejected",
            staged_path="/downloads/rejected",
        )

        self.assertEqual(
            db.get_retained_failure_paths(),
            {"/downloads/retained"},
        )

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
        now = datetime(2026, 6, 28, 12, 0, tzinfo=UTC)
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

    def test_plex_pin_rejects_invalid_status_without_mutating_row(self):
        import psycopg2.errors

        db = FakePipelineDB()
        pin_id = db.add_plex_added_at_pin(
            imported_path="A/B", original_added_at=1,
            rating_key=None, request_id=None)
        before = copy.deepcopy(db.plex_added_at_pins[0])
        with self.assertRaises(psycopg2.errors.CheckViolation):
            db.mark_plex_added_at_pin(
                pin_id, status=cast(Any, "stranded"),
                reconciled_at=datetime.now(UTC))
        self.assertEqual(db.plex_added_at_pins[0], before)

    def test_plex_pin_prune_matches_strict_terminal_age_contract(self):
        db = FakePipelineDB()
        cutoff = datetime(2026, 7, 11, tzinfo=UTC)
        for status, reconciled_at in (
            ("done", cutoff - timedelta(seconds=1)),
            ("skipped", cutoff),
            ("pending", cutoff - timedelta(days=365)),
        ):
            pin_id = db.add_plex_added_at_pin(
                imported_path=status, original_added_at=1,
                rating_key=None, request_id=None)
            db.plex_added_at_pins[pin_id - 1].update(
                status=status, reconciled_at=reconciled_at)

        removed = db.prune_terminal_plex_added_at_pins(older_than=cutoff)

        self.assertEqual(removed, 1)
        self.assertEqual(
            [row["status"] for row in db.plex_added_at_pins],
            ["skipped", "pending"],
        )

    def test_jellyfin_date_created_pin_add_get_pending_and_mark(self):
        """The fake mirrors migration-046 semantics: monotonic ids, pending
        filtered by status + captured_before cutoff, mark moves it terminal."""
        db = FakePipelineDB()
        now = datetime(2026, 7, 10, 12, 0, tzinfo=UTC)
        pin_id = db.add_jellyfin_date_created_pin(
            imported_path="Muse/2026 - The Wow! Signal",
            original_date_created="2026-04-26T18:31:04.4425337Z",
            album_item_id="alb-1",
            children_item_ids=["tr-1", "tr-2"],
            request_id=8812,
        )
        self.assertEqual(pin_id, 1)
        # Force a deterministic capture time in the past, then read pending.
        db.jellyfin_date_created_pins[0]["captured_at"] = now - timedelta(minutes=10)
        pending = db.get_pending_jellyfin_date_created_pins(
            captured_before=now, limit=100)
        self.assertEqual(len(pending), 1)
        row = pending[0]
        self.assertEqual(row["original_date_created"], "2026-04-26T18:31:04.4425337Z")
        self.assertEqual(row["album_item_id"], "alb-1")
        self.assertEqual(row["children_item_ids"], ["tr-1", "tr-2"])
        self.assertEqual(row["request_id"], 8812)
        self.assertEqual(row["status"], "pending")
        # A cutoff before the capture excludes the pin (settle-window guard).
        self.assertEqual(
            db.get_pending_jellyfin_date_created_pins(
                captured_before=now - timedelta(hours=1), limit=100),
            [])
        # Marking terminal removes it from pending.
        db.mark_jellyfin_date_created_pin(pin_id, status="expired", reconciled_at=now)
        self.assertEqual(
            db.get_pending_jellyfin_date_created_pins(captured_before=now, limit=100),
            [])
        self.assertEqual(db.jellyfin_date_created_pins[0]["status"], "expired")
        self.assertEqual(db.jellyfin_date_created_pins[0]["reconciled_at"], now)

    def test_jellyfin_pin_rejects_invalid_status_without_mutating_row(self):
        import psycopg2.errors

        db = FakePipelineDB()
        pin_id = db.add_jellyfin_date_created_pin(
            imported_path="A/B",
            original_date_created="2000-01-01T00:00:00Z",
            album_item_id="album", children_item_ids=[], request_id=None)
        before = copy.deepcopy(db.jellyfin_date_created_pins[0])
        with self.assertRaises(psycopg2.errors.CheckViolation):
            db.mark_jellyfin_date_created_pin(
                pin_id, status=cast(Any, "stranded"),
                reconciled_at=datetime.now(UTC))
        self.assertEqual(db.jellyfin_date_created_pins[0], before)

    def test_jellyfin_pin_prune_matches_strict_terminal_age_contract(self):
        db = FakePipelineDB()
        cutoff = datetime(2026, 7, 11, tzinfo=UTC)
        for status, reconciled_at in (
            ("done", cutoff - timedelta(seconds=1)),
            ("skipped", cutoff - timedelta(days=1)),
            ("expired", cutoff),
            ("pending", cutoff - timedelta(days=365)),
        ):
            pin_id = db.add_jellyfin_date_created_pin(
                imported_path=status,
                original_date_created="2000-01-01T00:00:00Z",
                album_item_id=status, children_item_ids=[], request_id=None)
            db.jellyfin_date_created_pins[pin_id - 1].update(
                status=status, reconciled_at=reconciled_at)

        removed = db.prune_terminal_jellyfin_date_created_pins(
            older_than=cutoff)

        self.assertEqual(removed, 2)
        self.assertEqual(
            [row["status"] for row in db.jellyfin_date_created_pins],
            ["expired", "pending"],
        )

    def test_log_download_rejects_non_canonical_outcome(self):
        """Mirror of download_log_outcome_check — the fake must reject
        exactly what production rejects (test-fidelity Rule A/B; the
        #146 grace escape shipped outcome='error' past a permissive
        fake and crashed on the real CHECK constraint)."""
        import psycopg2.errors
        db = FakePipelineDB()
        for outcome in ("error", "have_analysis_errors"):
            with self.subTest(outcome=outcome), self.assertRaises(psycopg2.errors.CheckViolation):
                db.log_download(42, outcome=outcome)

    def test_log_download_accepts_have_analysis_error(self):
        db = FakePipelineDB()
        log_id = db.log_download(42, outcome="have_analysis_error")

        self.assertEqual(log_id, 1)
        self.assertEqual(db.download_logs[0].outcome, "have_analysis_error")

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


# ---------------------------------------------------------------------------
# Field resolutions (migration 030) — fake parity
# ---------------------------------------------------------------------------


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
        from lib.pipeline_db import CURSOR_UPDATE_WRAPPED, ConsumedAttemptInput
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
        with self.assertRaises(ValueError):
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
        with self.assertRaises(ValueError):
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

    def _seed_searchable(
        self,
        db: FakePipelineDB,
        request_id: int,
        *,
        created_at: datetime,
        attempts: int = 1,
        title: str | None = None,
    ) -> int:
        db.seed_request(make_request_row(
            id=request_id,
            mb_release_id=f"scheduler-{request_id}",
            album_title=title or f"Album {request_id}",
            created_at=created_at,
            search_attempts=attempts,
            download_attempts=attempts,
            validation_attempts=attempts,
        ))
        self._make_active(db, request_id, "g1")
        return request_id

    def test_page_size_must_leave_capacity_for_both_cohorts(self):
        db = FakePipelineDB()
        for page_size in (-1, 0, 1):
            with self.subTest(page_size=page_size), self.assertRaisesRegex(ValueError, "at least 2"):
                db.get_wanted_searchable("g1", limit=page_size)

    def test_priority_capacity_and_bidirectional_borrowing(self):
        now = datetime(2026, 7, 20, 4, 0, tzinfo=UTC)

        db = FakePipelineDB()
        new_ids = {
            self._seed_searchable(
                db, index + 1, created_at=now - timedelta(hours=1))
            for index in range(2)
        }
        established_ids = {
            self._seed_searchable(
                db, index + 10, created_at=now - timedelta(days=2))
            for index in range(20)
        }
        selected = {
            int(row["id"])
            for row in db.get_wanted_searchable("g1", limit=16, now=now)
        }
        self.assertEqual(new_ids & selected, new_ids)
        self.assertEqual(len(established_ids & selected), 14)

        db = FakePipelineDB()
        new_ids = {
            self._seed_searchable(
                db, index + 1, created_at=now - timedelta(hours=1))
            for index in range(20)
        }
        established_ids = {
            self._seed_searchable(
                db, index + 100, created_at=now - timedelta(days=2))
            for index in range(2)
        }
        selected = {
            int(row["id"])
            for row in db.get_wanted_searchable("g1", limit=16, now=now)
        }
        self.assertEqual(len(new_ids & selected), 14)
        self.assertEqual(established_ids & selected, established_ids)

    def test_small_page_keeps_proportional_established_floor(self):
        now = datetime(2026, 7, 20, 4, 0, tzinfo=UTC)
        db = FakePipelineDB()
        new_ids = {
            self._seed_searchable(
                db, index + 1, created_at=now - timedelta(hours=1))
            for index in range(2)
        }
        established_ids = {
            self._seed_searchable(
                db, index + 100, created_at=now - timedelta(days=2))
            for index in range(5)
        }

        selected = {
            int(row["id"])
            for row in db.get_wanted_searchable("g1", limit=5, now=now)
        }

        self.assertEqual(len(new_ids & selected), 1)
        self.assertEqual(len(established_ids & selected), 4)

    def test_blacklist_cannot_consume_reserved_capacity(self):
        now = datetime(2026, 7, 20, 4, 0, tzinfo=UTC)
        db = FakePipelineDB()
        blocked_ids = {
            self._seed_searchable(
                db,
                index + 1,
                created_at=now - timedelta(hours=1),
                title=f"Blocked {index}",
            )
            for index in range(4)
        }
        allowed_new = self._seed_searchable(
            db, 10, created_at=now - timedelta(hours=1), title="Allowed")
        established_ids = {
            self._seed_searchable(
                db, index + 100, created_at=now - timedelta(days=2))
            for index in range(20)
        }

        selected = {
            int(row["id"])
            for row in db.get_wanted_searchable(
                "g1",
                limit=16,
                title_blacklist=("blocked",),
                now=now,
            )
        }

        self.assertFalse(blocked_ids & selected)
        self.assertIn(allowed_new, selected)
        self.assertEqual(len(established_ids & selected), 15)

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
        from datetime import datetime, timedelta
        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="A", album_title="B", source="request",
            mb_release_id="bo")
        self._make_active(db, rid, "g1")
        db.update_request_fields(
            rid,
            next_retry_after=datetime.now(UTC) + timedelta(hours=1),
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
                download_log_id=1,
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

    def test_cancel_false_return_keeps_only_rejected_transfer_resident(self):
        """Per-ID cancellation outcomes preserve the fake's live state."""
        slskd = FakeSlskdAPI()
        for transfer_id in ("tid-false", "tid-success"):
            slskd.add_transfer(
                username="user1", directory="Music\\Album",
                filename=f"Music\\Album\\{transfer_id}.flac",
                id=transfer_id, state="Completed, Succeeded",
            )
        slskd.transfers.cancel_download_results_by_id["tid-false"] = False

        self.assertFalse(slskd.transfers.cancel_download(
            "user1", "tid-false", remove=True))
        self.assertTrue(slskd.transfers.cancel_download(
            "user1", "tid-success", remove=True))

        remaining_ids = {
            transfer.id
            for user in slskd.transfers.get_all_downloads()
            for directory in user.directories
            for transfer in directory.files
        }
        self.assertEqual(remaining_ids, {"tid-false"})

    def test_user_directories_record_results_and_errors(self):
        slskd = FakeSlskdAPI()
        directory = [{"directory": "Music\\Album", "files": []}]
        slskd.users.set_directory("user1", "Music\\Album", directory)
        slskd.users.set_directory_error(
            "user1",
            "Music\\Broken",
            RuntimeError("Peer offline"),
        )

        self.assertEqual(slskd.users.directory("user1", "Music\\Album"), directory)
        with self.assertRaises(RuntimeError):
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

    def test_search_text_error_by_query_targets_exact_searchtext(self):
        """Issue #1090 NIT-9: per-searchText keyed injection is
        independent of call order/count across OTHER distinct
        searchText values -- a candidate that never calls search_text at
        all (e.g. an empty-artist_name guard) cannot desynchronise a
        keyed queue meant for a different candidate's text."""
        slskd = FakeSlskdAPI()
        slskd.searches.search_text_error_by_query["Artist A"] = [
            RuntimeError("A fails once"), None,
        ]
        slskd.searches.search_text_error_by_query["Artist B"] = [
            RuntimeError("B always fails"),
        ]
        # B's queue is untouched by A's calls.
        with self.assertRaises(RuntimeError) as caught_a1:
            slskd.searches.search_text(searchText="Artist A", responseLimit=1000)
        self.assertEqual(str(caught_a1.exception), "A fails once")
        result = slskd.searches.search_text(searchText="Artist A", responseLimit=1000)
        self.assertIn("id", result)
        with self.assertRaises(RuntimeError) as caught_b:
            slskd.searches.search_text(searchText="Artist B", responseLimit=1000)
        self.assertEqual(str(caught_b.exception), "B always fails")

    def test_search_text_error_by_query_takes_priority_over_blanket_error(self):
        """Issue #1112: with the flat-FIFO ``search_text_error_sequence``
        mechanism removed, the by-query queue is the only per-call
        injection left besides the blanket ``search_text_error`` poison --
        confirm it still wins when both are configured for the same
        query."""
        slskd = FakeSlskdAPI()
        slskd.searches.search_text_error_by_query["Artist A"] = [
            RuntimeError("keyed error"),
        ]
        slskd.searches.search_text_error = RuntimeError("blanket error")
        with self.assertRaises(RuntimeError) as caught:
            slskd.searches.search_text(searchText="Artist A", responseLimit=1000)
        self.assertEqual(str(caught.exception), "keyed error")

    def test_search_text_error_by_query_exhausted_falls_back_to_blanket_error(
        self,
    ):
        """Once a query's own queue is exhausted, ``search_text_error``
        (if set) resumes poisoning THAT query's later calls -- the
        per-query queue is a prefix override, not a replacement for the
        blanket-error knob."""
        slskd = FakeSlskdAPI()
        slskd.searches.search_text_error_by_query["Artist A"] = [None]
        slskd.searches.search_text_error = RuntimeError("blanket failure")
        # First call consumes the queue's lone None -- succeeds.
        slskd.searches.search_text(searchText="Artist A", responseLimit=1000)
        # Queue now empty -- falls back to search_text_error.
        with self.assertRaises(RuntimeError):
            slskd.searches.search_text(searchText="Artist A", responseLimit=1000)


class TestFakeSlskdServer(unittest.TestCase):
    """Self-test for the FakeSlskdServer stub introduced for issue #1090."""

    def test_defaults_to_ready(self):
        from lib.slskd_client import SlskdServerState
        slskd = FakeSlskdAPI()
        state = slskd.server.state()
        self.assertIsInstance(state, SlskdServerState)
        self.assertTrue(state.is_connected)
        self.assertTrue(state.is_logged_in)
        self.assertEqual(slskd.server.state_calls, 1)

    def test_set_ready_reports_reconnect_window(self):
        slskd = FakeSlskdAPI()
        slskd.server.set_ready(is_connected=True, is_logged_in=False)
        state = slskd.server.state()
        self.assertTrue(state.is_connected)
        self.assertFalse(state.is_logged_in)

    def test_state_error_propagates(self):
        slskd = FakeSlskdAPI()
        slskd.server.state_error = RuntimeError("server endpoint down")
        with self.assertRaises(RuntimeError):
            slskd.server.state()


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
        from lib.import_queue import IMPORT_JOB_FORCE

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="wanted"))
        first = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            dedupe_key="force:42",
            payload={"download_log_id": 1, "failed_path": "/tmp/force"},
        )
        duplicate = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            dedupe_key="force:42",
            payload={"download_log_id": 1, "failed_path": "/tmp/force"},
        )
        self.assertEqual(first.id, duplicate.id)
        self.assertTrue(duplicate.deduped)
        self.assertEqual(db.count_import_jobs_by_status(), {"queued": 1})
        db.mark_import_job_preview_importable(
            first.id,
            preview_result={"verdict": "would_import"},
            message="ready",
        )

        claimed = claim_next_import_job(db, worker_id="fake-worker")
        assert claimed is not None
        self.assertEqual(claimed.status, "running")
        self.assertEqual(claimed.attempts, 1)
        self.assertEqual(claimed.worker_id, "fake-worker")

        requeued = db.recover_running_import_jobs(
            requeue_message="retry",
            recovery_message="recovery required",
        )
        self.assertEqual([job.id for job in requeued], [claimed.id])
        self.assertEqual(requeued[0].status, "queued")
        self.assertIsNone(requeued[0].worker_id)

        claimed = claim_next_import_job(db, worker_id="fake-worker-2")
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
            IMPORT_JOB_FORCE,
            request_id=42,
            dedupe_key="force:42",
            payload={"download_log_id": 1, "failed_path": "/tmp/force"},
        )
        self.assertNotEqual(first.id, later.id)
        failed = db.mark_import_job_failed(
            later.id,
            error="boom",
            message="failed",
        )
        assert failed is not None
        self.assertEqual(failed.status, "failed")

    def test_automation_commands_require_exact_owner_stage_and_lease(self):
        from lib.import_execution import (
            ExecutionLeaseSnapshot,
            ProcessIdentity,
        )

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            mb_release_id="fake-owner-lease",
            status="wanted",
        ))
        job = handoff_automation_owner(
            db,
            42,
            canonical_path="/processing/albums/fake-owner-lease",
        )
        preview_lease = ExecutionLeaseSnapshot(
            host_boot_id="boot-a",
            invocation_id="preview-a",
            systemd_unit="cratedigger-import-preview.service",
            worker=ProcessIdentity(pid=101, start_ticks=1001),
        )
        stale_preview_lease = ExecutionLeaseSnapshot(
            host_boot_id="boot-a",
            invocation_id="preview-stale",
            systemd_unit="cratedigger-import-preview.service",
            worker=ProcessIdentity(pid=102, start_ticks=1002),
        )

        self.assertIsNone(claim_next_import_preview_job(db, worker_id="no-lease"))
        claimed_preview = claim_next_import_preview_job(db, worker_id="preview",
        execution_lease=preview_lease,)
        assert claimed_preview is not None
        self.assertEqual(
            claimed_preview.execution_invocation_id,
            preview_lease.invocation_id,
        )
        self.assertEqual(db.requeue_stale_import_preview_jobs(
            older_than=timedelta(seconds=-1),
            message="heartbeat age is not automation proof",
        ), [])
        self.assertEqual(db.requeue_running_import_preview_jobs(
            message="process restart is not automation proof",
        ), [])
        self.assertFalse(db.heartbeat_import_job_preview(
            job.id,
            expected_execution_lease=stale_preview_lease,
        ))
        self.assertFalse(db.set_import_job_candidate_evidence(
            job.id,
            77,
            expected_execution_lease=stale_preview_lease,
        ))
        self.assertTrue(db.set_import_job_candidate_evidence(
            job.id,
            77,
            expected_execution_lease=preview_lease,
        ))
        self.assertIsNotNone(db.mark_import_job_preview_importable(
            job.id,
            preview_result={"verdict": "would_import"},
            expected_execution_lease=preview_lease,
        ))

        importer_lease = ExecutionLeaseSnapshot(
            host_boot_id="boot-a",
            invocation_id="importer-a",
            systemd_unit="cratedigger-importer.service",
            worker=ProcessIdentity(pid=201, start_ticks=2001),
        )
        claimed_import = claim_next_import_job(db, worker_id="importer",
        execution_lease=importer_lease,)
        assert claimed_import is not None
        self.assertFalse(db.heartbeat_import_job(
            job.id,
            expected_execution_lease=preview_lease,
        ))
        self.assertTrue(db.heartbeat_import_job(
            job.id,
            expected_execution_lease=importer_lease,
        ))

        # A wrong stage/status cannot borrow even the exact execution lease.
        db._requests[42]["status"] = "wanted"
        self.assertFalse(db.heartbeat_import_job(
            job.id,
            expected_execution_lease=importer_lease,
        ))
        db._requests[42]["status"] = "processing"


    def test_automation_startup_recovery_requires_exact_dead_proof(self):
        from lib.import_execution import (
            ExecutionLeaseSnapshot,
            ExecutionLivenessDecision,
            ExecutionLivenessEvidence,
            ProcessIdentity,
        )

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=44,
            mb_release_id="fake-startup-recovery",
            status="wanted",
        ))
        job = handoff_automation_owner(db, 44)
        lease = ExecutionLeaseSnapshot(
            host_boot_id="boot-old",
            invocation_id="preview-old",
            systemd_unit="cratedigger-import-preview.service",
            worker=ProcessIdentity(pid=501, start_ticks=5001),
        )
        assert claim_next_import_preview_job(db, worker_id="preview",
        execution_lease=lease,) is not None

        exact_evidence = ExecutionLivenessEvidence(
            lease=lease,
            current_host_boot_id="boot-new",
            boot_error=None,
            worker=None,
            beets=None,
            invocation=None,
            cgroup=None,
        )
        live = ExecutionLivenessDecision(
            status="live",
            reason="still alive",
            evidence=exact_evidence,
        )
        self.assertIsNone(db.recover_automation_import_job(
            job.id,
            expected_execution_lease=lease,
            decision=live,
            requeue_message="requeue",
            recovery_message="operator recovery",
        ))

        stale_lease = ExecutionLeaseSnapshot(
            host_boot_id="boot-old",
            invocation_id="preview-other",
            systemd_unit=lease.systemd_unit,
            worker=lease.worker,
        )
        stale_dead = ExecutionLivenessDecision(
            status="dead",
            reason="different invocation ended",
            evidence=ExecutionLivenessEvidence(
                lease=stale_lease,
                current_host_boot_id="boot-new",
                boot_error=None,
                worker=None,
                beets=None,
                invocation=None,
                cgroup=None,
            ),
        )
        self.assertIsNone(db.recover_automation_import_job(
            job.id,
            expected_execution_lease=lease,
            decision=stale_dead,
            requeue_message="requeue",
            recovery_message="operator recovery",
        ))

        dead = ExecutionLivenessDecision(
            status="dead",
            reason="prior boot ended",
            evidence=exact_evidence,
        )
        recovered = db.recover_automation_import_job(
            job.id,
            expected_execution_lease=lease,
            decision=dead,
            requeue_message="requeue",
            recovery_message="operator recovery",
        )
        assert recovered is not None
        self.assertEqual(recovered.status, "queued")
        self.assertEqual(recovered.preview_status, "waiting")
        self.assertIsNone(recovered.execution_invocation_id)


    def test_requeue_import_job_for_preview_flips_running_back_to_waiting(self):
        from lib.import_queue import IMPORT_JOB_FORCE

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="wanted"))
        job = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            dedupe_key="force:requeue-fake",
            payload={"download_log_id": 1, "failed_path": "/tmp/force"},
        )
        db.mark_import_job_preview_importable(
            job.id,
            preview_result={"verdict": "would_import"},
            message="ready",
        )
        claimed = claim_next_import_job(db, worker_id="importer")
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

        # Candidate selection owns the requeue delay.
        self.assertIsNone(claim_next_import_preview_job(
            db, worker_id="preview-too-soon"))
        row = next(row for row in db._import_jobs if row["id"] == claimed.id)
        row["updated_at"] -= timedelta(seconds=61)
        preview = claim_next_import_preview_job(db, worker_id="preview-1")
        assert preview is not None
        self.assertEqual(preview.id, claimed.id)

    def test_requeue_import_job_for_preview_idempotent_when_not_running(self):
        from lib.import_queue import IMPORT_JOB_FORCE

        db = FakePipelineDB()
        job = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            dedupe_key="force:requeue-fake-idem",
            payload={"download_log_id": 1, "failed_path": "/tmp/force"},
        )
        # Not yet claimed by importer (preview_status='waiting', status='queued').
        result = db.requeue_import_job_for_preview(
            job.id,
            reason="not running",
        )
        self.assertIsNone(result)

    def test_import_job_queue_defaults_to_preview_waiting(self):
        from lib.import_queue import IMPORT_JOB_FORCE

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="wanted"))
        queued = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            dedupe_key="force:fresh",
            payload={"download_log_id": 1, "failed_path": "/tmp/force"},
        )

        self.assertEqual(queued.preview_status, "waiting")
        self.assertIsNone(queued.preview_message)
        self.assertIsNone(queued.preview_completed_at)
        self.assertIsNone(queued.importable_at)
        # Preview worker can claim it; importer cannot.
        self.assertIsNone(claim_next_import_job(db, worker_id="importer"))
        claimed = claim_next_import_preview_job(db, worker_id="preview")
        assert claimed is not None
        self.assertEqual(claimed.id, queued.id)

    def test_log_download_derives_validation_projection_like_postgres(self):
        from lib.quality import ValidationResult

        db = FakePipelineDB()
        validation_result = ValidationResult(
            distance=0.0,
            scenario="untracked_audio",
        ).to_json()
        db.log_download(
            42,
            outcome="rejected",
            validation_result=validation_result,
        )

        log = db.download_logs[0]
        self.assertEqual(log.beets_distance, 0.0)
        self.assertEqual(log.beets_scenario, "untracked_audio")
        self.assertEqual(log.validation_result, validation_result)

    def test_log_download_derives_custom_envelope_scenario_like_postgres(self):
        db = FakePipelineDB()
        validation_result = {
            "scenario": "curator_ban",
            "hashes_recorded": 2,
        }
        db.log_download(
            42,
            outcome="curator_ban",
            validation_result=validation_result,
        )

        log = db.download_logs[0]
        self.assertEqual(log.beets_scenario, "curator_ban")
        self.assertIsNone(log.beets_distance)
        self.assertIs(log.validation_result, validation_result)

    def test_log_download_explicit_metadata_requires_missing_envelope_key(self):
        import msgspec

        from lib.quality import MeasurementFailure, ValidationResult

        db = FakePipelineDB()
        payload = MeasurementFailure(
            reason="measurement_crashed",
            detail="ffmpeg failed",
        )
        db.log_download(
            42,
            outcome="measurement_failed",
            beets_scenario="measurement_failed",
            validation_result=msgspec.json.encode(payload).decode(),
        )
        self.assertEqual(db.download_logs[-1].beets_scenario,
                         "measurement_failed")

        validation_result = ValidationResult(
            distance=0.1,
            scenario="high_distance",
        ).to_json()
        with self.assertRaisesRegex(ValueError, "beets_distance"):
            db.log_download(
                42,
                outcome="rejected",
                beets_distance=0.2,
                validation_result=validation_result,
            )
        with self.assertRaisesRegex(ValueError, "beets_scenario"):
            db.log_download(
                42,
                outcome="rejected",
                beets_scenario="wrong_value",
                validation_result=validation_result,
            )

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

    def test_unfindable_run_metrics_stub_round_trips_and_feeds_dashboard(self):
        db = FakePipelineDB()

        empty = db.get_pipeline_dashboard_metrics()["unfindable"]
        self.assertEqual(empty["recent_runs"], [])
        self.assertIsNone(empty["backlog_trend"]["current_backlog"])

        first_id = db.record_unfindable_run_metrics(
            cohort_total=1301, due_backlog_at_start=900,
            batch_limit=240, candidates_processed=240, probes_attempted=240,
            categorised_count=5, downgraded_count=1, no_change_count=210,
            probe_failed_count=24, breaker_tripped=False,
            duration_seconds=6900.0,
        )
        second_id = db.record_unfindable_run_metrics(
            cohort_total=1301, due_backlog_at_start=686,
            batch_limit=240, candidates_processed=93, probes_attempted=90,
            probe_failed_count=90, not_due_count=0,
            request_not_found_count=3, breaker_tripped=True,
            duration_seconds=1800.0,
        )
        self.assertEqual((first_id, second_id), (1, 2))

        rows = db.get_unfindable_run_metrics(limit=5)
        self.assertEqual(len(rows), 2)
        # Newest first, and every field of the second call round-trips.
        newest = rows[0]
        self.assertEqual(newest["id"], second_id)
        self.assertEqual(newest["due_backlog_at_start"], 686)
        self.assertEqual(newest["candidates_processed"], 93)
        self.assertEqual(newest["probes_attempted"], 90)
        self.assertEqual(newest["probe_failed_count"], 90)
        self.assertEqual(newest["request_not_found_count"], 3)
        self.assertTrue(newest["breaker_tripped"])
        self.assertEqual(newest["duration_seconds"], 1800.0)
        self.assertEqual(newest["categorised_count"], 0)

        dashboard = db.get_pipeline_dashboard_metrics()["unfindable"]
        self.assertEqual(len(dashboard["recent_runs"]), 2)
        self.assertEqual(
            dashboard["recent_runs"][0]["due_backlog_at_start"], 686)
        self.assertEqual(dashboard["backlog_trend"]["current_backlog"], 686)
        self.assertEqual(
            [pt["due_backlog_at_start"]
             for pt in dashboard["backlog_trend"]["series"]],
            [900, 686],
        )

    def test_record_unfindable_run_metrics_rejects_non_partitioning_counts(
        self,
    ):
        """Mirror of unfindable_run_metrics_partition_check (migration
        077, #1112 review round 2 R5) -- the six RESULT_* outcome counts
        must sum to candidates_processed exactly."""
        import psycopg2.errors
        db = FakePipelineDB()
        with self.assertRaises(psycopg2.errors.CheckViolation):
            db.record_unfindable_run_metrics(
                cohort_total=10, due_backlog_at_start=5,
                batch_limit=5, candidates_processed=5, probes_attempted=5,
                breaker_tripped=False, duration_seconds=1.0,
                categorised_count=1, no_change_count=1,  # sums to 2, not 5
            )

    def test_record_unfindable_run_metrics_rejects_wrong_probes_attempted(
        self,
    ):
        """Mirror of unfindable_run_metrics_probes_attempted_check
        (migration 077, #1112 review round 2 R5) -- probes_attempted
        must equal candidates_processed minus not_due_count minus
        request_not_found_count."""
        import psycopg2.errors
        db = FakePipelineDB()
        with self.assertRaises(psycopg2.errors.CheckViolation):
            db.record_unfindable_run_metrics(
                cohort_total=10, due_backlog_at_start=5,
                batch_limit=5, candidates_processed=5,
                probes_attempted=5,  # should be 5 - 0 - 2 = 3
                breaker_tripped=False, duration_seconds=1.0,
                no_change_count=3, request_not_found_count=2,
            )

    def test_import_job_preview_methods_mirror_core_lifecycle(self):
        from lib.import_queue import IMPORT_JOB_FORCE

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="wanted"))
        db.seed_request(make_request_row(id=43, status="wanted"))
        queued = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            dedupe_key="force:preview",
            payload={"download_log_id": 1, "failed_path": "/tmp/force"},
        )
        self.assertEqual(queued.preview_status, "waiting")

        claimed = claim_next_import_preview_job(db, worker_id="fake-preview")
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
            IMPORT_JOB_FORCE,
            request_id=43,
            dedupe_key="force:preview-reject",
            payload={"download_log_id": 1, "failed_path": "/tmp/reject"},
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
        now = datetime.now(UTC)
        db.seed_request(make_request_row(
            id=1, status="wanted", created_at=now + timedelta(seconds=2)))
        db.seed_request(make_request_row(
            id=2, status="wanted", created_at=now))
        rows = db.get_by_status("wanted")
        self.assertEqual([r["id"] for r in rows], [2, 1])

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

    def test_get_wrong_matches_newer_terminal_corrupt_hides_same_path(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, artist_name="A", album_title="B"))
        db.seed_request(make_request_row(id=2, artist_name="C", album_title="D"))
        db.log_download(1, soulseek_username="older-legitimate", outcome="rejected",
                        validation_result={"failed_path": "/same", "scenario": "high_distance"})
        db.log_download(1, soulseek_username="newer-corrupt", outcome="rejected",
                        validation_result={"failed_path": "/same", "scenario": "strong_match"},
                        import_result={"decision": "audio_corrupt"})
        other_id = db.log_download(2, soulseek_username="other-legitimate", outcome="rejected",
                                   validation_result={"failed_path": "/other", "scenario": "high_distance"})
        rows = db.get_wrong_matches()
        self.assertEqual(
            [(row["download_log_id"], row["soulseek_username"]) for row in rows],
            [(other_id, "other-legitimate")],
        )

    def test_get_wrong_matches_newer_legitimate_reuse_surfaces_after_corrupt(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, artist_name="A", album_title="B"))
        db.log_download(1, soulseek_username="older-corrupt", outcome="rejected",
                        validation_result={"failed_path": "/same", "scenario": "strong_match"},
                        import_result={"decision": "audio_corrupt"})
        newest_id = db.log_download(1, soulseek_username="newer-legitimate", outcome="rejected",
                                    validation_result={"failed_path": "/same", "scenario": "high_distance"})
        rows = db.get_wrong_matches()
        self.assertEqual(
            [(row["download_log_id"], row["soulseek_username"]) for row in rows],
            [(newest_id, "newer-legitimate")],
        )

    def test_get_wrong_matches_excludes_every_non_match_rejection_scenario(self):
        from lib.wrong_match_policy import WRONG_MATCH_EXCLUDED_REJECTION_SCENARIOS

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, artist_name="A", album_title="B"))
        db.log_download(1, outcome="rejected", validation_result={
            "failed_path": "/keep", "scenario": "high_distance"})
        for index, scenario in enumerate(
            sorted(WRONG_MATCH_EXCLUDED_REJECTION_SCENARIOS)
        ):
            db.log_download(1, outcome="rejected", validation_result={
                "failed_path": f"/drop-{index}", "scenario": scenario})

        rows = db.get_wrong_matches()
        self.assertEqual(len(rows), 1)
        validation_result = rows[0]["validation_result"]
        assert isinstance(validation_result, dict)
        self.assertEqual(validation_result["failed_path"], "/keep")

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
        stored = _json.loads(db.download_logs[0].validation_result)
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
        stored = _json.loads(db.download_logs[0].validation_result)
        self.assertNotIn("failed_path", stored)
        self.assertEqual(stored["x"], 1)

    def test_list_terminal_force_wrong_match_cleanup_jobs_mirrors_sql_predicate(
        self,
    ) -> None:
        """Issue #1122: the fake must select the same rows the real SQL does.

        Real-PG proof of the same predicate lives in
        ``tests/test_pipeline_db.py`` — this pins the fake against an
        IDENTICAL scenario matrix so the two never silently drift
        (test-fidelity.md's fake-vs-SQL predicate drift class). Covers the
        review-round corrections: MAJOR-1 (success-keyed, not
        presence-keyed), MAJOR-2/3 (the era-AND-lane marker excludes every
        historical/non-adjudicating shape by construction).
        """
        from lib.import_queue import IMPORT_JOB_FORCE, force_import_payload

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1))

        def _force_job(suffix: str):
            return db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=1,
                dedupe_key=f"force-wrong-match-predicate:{suffix}",
                payload=force_import_payload(
                    download_log_id=1,
                    failed_path="/tmp/predicate-source",
                ),
            )

        # -- completed arm --------------------------------------------------

        completed_missing = _force_job("completed-missing")
        db.mark_import_job_completed(
            completed_missing.id,
            result={
                "success": True, "message": "done", "deferred": False,
                "code": None, "post_commit_wrong_match_scenario": None,
            },
            message="done",
        )

        completed_failed_receipt = _force_job("completed-failed-receipt")
        db.mark_import_job_completed(
            completed_failed_receipt.id,
            result={
                "success": True, "message": "done", "deferred": False,
                "code": None, "post_commit_wrong_match_scenario": None,
                "wrong_match_dismissal": {
                    "success": False, "error": "path_unavailable: EACCES",
                },
            },
            message="done",
        )

        completed_successful_receipt = _force_job("completed-successful-receipt")
        db.mark_import_job_completed(
            completed_successful_receipt.id,
            result={
                "success": True, "message": "done", "deferred": False,
                "code": None, "post_commit_wrong_match_scenario": None,
                "wrong_match_dismissal": {"success": True},
            },
            message="done",
        )

        # -- failed arm -------------------------------------------------

        failed_missing = _force_job("failed-missing")
        db.mark_import_job_failed(
            failed_missing.id,
            error="beets rejected: audio_corrupt",
            result={
                "success": False, "message": "rejected", "deferred": False,
                "code": None,
                "post_commit_wrong_match_scenario": "audio_corrupt",
            },
            message="rejected",
        )

        failed_failed_receipt = _force_job("failed-failed-receipt")
        db.mark_import_job_failed(
            failed_failed_receipt.id,
            error="beets rejected: audio_corrupt",
            result={
                "success": False, "message": "rejected", "deferred": False,
                "code": None,
                "post_commit_wrong_match_scenario": "audio_corrupt",
                "cleanup": {
                    "success": False, "outcome": "deleted_operator_force_source",
                    "error": "path_unavailable: EACCES",
                },
            },
            message="rejected",
        )

        failed_successful_receipt = _force_job("failed-successful-receipt")
        db.mark_import_job_failed(
            failed_successful_receipt.id,
            error="beets rejected",
            result={
                "success": False, "message": "rejected", "deferred": False,
                "code": None,
                "post_commit_wrong_match_scenario": "high_distance",
                "cleanup": {
                    "success": True,
                    "outcome": "preserved_operator_force_source",
                },
            },
            message="rejected",
        )

        failed_requeue = _force_job("failed-requeue")
        db.mark_import_job_failed(
            failed_requeue.id,
            error="requeue failed",
            result={
                "success": False, "message": "requeue UPDATE failed",
                "deferred": False, "code": "requeue_failed",
                "post_commit_wrong_match_scenario": None,
            },
            message="requeue UPDATE failed",
        )

        failed_requeue_exhausted = _force_job("failed-requeue-exhausted")
        db.mark_import_job_failed(
            failed_requeue_exhausted.id,
            error="preview/import requeue budget exhausted",
            result={
                "success": False, "message": "budget exhausted",
                "deferred": False, "code": "requeue_exhausted",
                "post_commit_wrong_match_scenario": None,
            },
            message="budget exhausted",
        )

        failed_deferred = _force_job("failed-deferred")
        db.mark_import_job_failed(
            failed_deferred.id,
            error="Another import is already in progress",
            result={
                "success": False,
                "message": "Another import is already in progress",
                "deferred": True, "code": None,
                "post_commit_wrong_match_scenario": None,
            },
            message="Another import is already in progress",
        )

        # -- historical / non-adjudicating shapes (MAJOR-2/3) ------------

        historical_completed = _force_job("historical-completed-no-marker")
        db.mark_import_job_completed(
            historical_completed.id,
            result={"success": True},
            message="done",
        )

        historical_failed = _force_job("historical-failed-no-marker")
        db.mark_import_job_failed(
            historical_failed.id,
            error="RuntimeError: boom",
            result={"success": False},
            message="Executor crashed",
        )

        # A genuinely NULL ``result`` column has no public-API constructor
        # on the fake either (``mark_import_job_failed`` always writes
        # ``result or {}``) — reach into the fake's own row store directly,
        # mirroring the real-PG test's raw ``UPDATE ... result = NULL``.
        historical_null_result = _force_job("historical-null-result")
        db.mark_import_job_failed(historical_null_result.id, error="boom")
        for row in db._import_jobs:
            if row["id"] == historical_null_result.id:
                row["result"] = None
                break

        selected = {
            job.id
            for job in db.list_terminal_force_wrong_match_cleanup_jobs()
        }
        self.assertIn(completed_missing.id, selected)
        self.assertIn(completed_failed_receipt.id, selected)
        self.assertNotIn(completed_successful_receipt.id, selected)
        self.assertIn(failed_missing.id, selected)
        self.assertIn(failed_failed_receipt.id, selected)
        self.assertNotIn(failed_successful_receipt.id, selected)
        self.assertNotIn(failed_requeue.id, selected)
        self.assertNotIn(failed_requeue_exhausted.id, selected)
        self.assertNotIn(failed_deferred.id, selected)
        self.assertNotIn(historical_completed.id, selected)
        self.assertNotIn(historical_failed.id, selected)
        self.assertNotIn(historical_null_result.id, selected)

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
        now = datetime.now(UTC)
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
            2026, 5, 7, 23, 55, tzinfo=UTC,
        )
        # Sanity: the same instant in Perth-local is 2026-05-08 07:55.
        self.assertEqual(observed_at.astimezone(perth).date(),
                         date(2026, 5, 8))

        db.record_peer_observations(["alice"], observed_at=observed_at)

        with patch("tests.fakes.pipeline_db._utcnow") as fake_now:
            fake_now.return_value = datetime(
                2026, 5, 9, 5, 0, tzinfo=UTC,
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


class TestFakeActiveImportJobsForWrongMatch(unittest.TestCase):
    def test_matches_by_download_log_path_or_source_dir(self):
        from lib.import_queue import IMPORT_JOB_FORCE, force_import_payload

        db = FakePipelineDB()
        for request_id in (1, 2, 3, 42):
            db.seed_request(make_request_row(
                id=request_id,
                status="wanted",
            ))
        by_log = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=1,
            payload=force_import_payload(download_log_id=10, failed_path="/other"),
        )
        db.enqueue_import_job(
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
        claimed = claim_next_import_job(db, worker_id="w")
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

    A new kwarg on a real method can otherwise be silently swallowed if the
    fake accepts ``**kwargs``.
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
            "arm_request_creation_race",
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

    def test_album_mb_identities_round_trip(self) -> None:
        """#1093 item 1 — the retag divergence audit's read seam."""
        from lib.beets_db import BeetsAlbumIdentityRow

        beets = FakeBeetsDB()
        self.assertEqual(beets.list_album_mb_identities(), [])

        rows = [
            BeetsAlbumIdentityRow(
                album_id=7,
                mb_albumid="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                item_paths=("/library/Artist/Album/01.flac",),
            ),
            BeetsAlbumIdentityRow(album_id=8, mb_albumid="", item_paths=()),
        ]
        beets.set_album_mb_identities(rows)

        self.assertEqual(beets.list_album_mb_identities(), rows)
        # Returns a fresh list — callers mutating the result never poison
        # the fake's seeded state.
        beets.list_album_mb_identities().append(rows[0])
        self.assertEqual(beets.list_album_mb_identities(), rows)

    def test_get_album_mb_identity_looks_up_by_id(self) -> None:
        """#1142 — the per-album retag recheck's narrow read seam."""
        from lib.beets_db import BeetsAlbumIdentityRow

        beets = FakeBeetsDB()
        rows = [
            BeetsAlbumIdentityRow(
                album_id=7,
                mb_albumid="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                item_paths=("/library/Artist/Album/01.flac",),
            ),
            BeetsAlbumIdentityRow(album_id=8, mb_albumid="", item_paths=()),
        ]
        beets.set_album_mb_identities(rows)

        self.assertEqual(beets.get_album_mb_identity(7), rows[0])
        self.assertEqual(beets.get_album_mb_identity(8), rows[1])
        self.assertIsNone(beets.get_album_mb_identity(999))

    def test_current_resolver_preserves_cardinality_and_topology(self) -> None:
        from lib.beets_db import CurrentBeetsAmbiguous, CurrentBeetsUnique
        from lib.release_identity import ReleaseIdentity

        identity = ReleaseIdentity(
            source="musicbrainz",
            release_id="11111111-1111-1111-1111-111111111111",
        )
        beets = FakeBeetsDB(library_root="/library")
        beets.set_album_ids_for_release(identity.release_id, [7])
        beets.set_item_paths(identity.release_id, [(70, "Artist/Album/01.flac")])
        unique = beets.resolve_current_release(identity)
        self.assertIsInstance(unique, CurrentBeetsUnique)
        assert isinstance(unique, CurrentBeetsUnique)
        self.assertEqual(unique.album_path, "/library/Artist/Album")

        beets.set_album_ids_for_release(identity.release_id, [7, 8])
        ambiguous = beets.resolve_current_release(identity)
        self.assertIsInstance(ambiguous, CurrentBeetsAmbiguous)
        assert isinstance(ambiguous, CurrentBeetsAmbiguous)
        self.assertEqual(ambiguous.reason, "multiple_matches")

        beets.set_album_ids_for_release(identity.release_id, [7])
        for invalid in ("", None, "bad\x00path.flac", "../outside.flac"):
            with self.subTest(invalid=invalid):
                beets.set_item_paths(identity.release_id, [(70, invalid)])
                poisoned = beets.resolve_current_release(identity)
                self.assertIsInstance(poisoned, CurrentBeetsAmbiguous)
                assert isinstance(poisoned, CurrentBeetsAmbiguous)
                self.assertEqual(poisoned.reason, "invalid_path")

    def test_resolve_current_release_error_mirrors_beets_authority_failure(
        self,
    ) -> None:
        """#1089 MINOR-5 (test-fidelity Rule B): the fake must be able to
        RAISE, not just return a shape, so ``MergeRekeyService``'s Beets-
        authority classify-or-reraise boundary is exercised with a real
        exception instance rather than a synthetic stand-in."""
        import sqlite3

        from lib.release_identity import ReleaseIdentity

        identity = ReleaseIdentity(
            source="musicbrainz",
            release_id="11111111-1111-1111-1111-111111111111",
        )
        beets = FakeBeetsDB()
        locked = sqlite3.OperationalError("database is locked")
        locked.sqlite_errorcode = sqlite3.SQLITE_LOCKED
        beets.set_resolve_current_release_error(identity.release_id, locked)

        with self.assertRaises(sqlite3.OperationalError):
            beets.resolve_current_release(identity)
        self.assertEqual(beets.resolve_current_release_calls, [identity])

        # A different release id is unaffected — the error is keyed, not
        # global.
        other = ReleaseIdentity(
            source="musicbrainz",
            release_id="22222222-2222-2222-2222-222222222222",
        )
        beets.set_album_ids_for_release(other.release_id, [])
        beets.resolve_current_release(other)  # does not raise

    def test_discogs_alias_reseed_replaces_the_canonical_current_snapshot(
        self,
    ) -> None:
        from lib.beets_db import AlbumInfo, CurrentBeetsUnique
        from lib.release_identity import ReleaseIdentity

        beets = FakeBeetsDB(library_root="/library")
        for release_id, album_id, path in (
            ("0012856590", 7, "/library/stale"),
            ("12856590", 8, "/library/current"),
        ):
            beets.set_album_info(release_id, AlbumInfo(
                album_id=album_id,
                track_count=1,
                min_bitrate_kbps=245,
                avg_bitrate_kbps=245,
                median_bitrate_kbps=245,
                is_cbr=True,
                album_path=path,
                format="MP3",
            ))

        identity = ReleaseIdentity.from_id("0012856590")
        assert identity is not None
        current = beets.resolve_current_release(identity)
        self.assertIsInstance(current, CurrentBeetsUnique)
        assert isinstance(current, CurrentBeetsUnique)
        self.assertEqual(current.album_id, 8)
        self.assertEqual(current.album_path, "/library/current")
        self.assertEqual(
            beets.get_album_info("0012856590", None).album_id,
            8,
        )

    def test_check_mbids_detail_returns_seeded_rows_only(self) -> None:
        beets = FakeBeetsDB()
        beets.set_mbid_detail(
            "mbid-1",
            {
                "beets_tracks": 11,
                "beets_bitrate": 194,
                "beets_avg_bitrate": 288,
            },
        )
        out = beets.check_mbids_detail(["mbid-1", "mbid-2"])
        self.assertEqual(out, {"mbid-1": {
            "beets_tracks": 11,
            "beets_format": None,
            "beets_bitrate": 194,
            "beets_avg_bitrate": 288,
            "beets_samplerate": None,
            "beets_bitdepth": None,
        }})
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

    def test_exact_album_projection_matches_each_cross_source_identity(self) -> None:
        beets = FakeBeetsDB()
        album = {
            "id": 7,
            "album": "Dual-tagged pressing",
            "mb_albumid": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "discogs_albumid": "12856590",
        }
        beets.set_albums_by_artist("X", [album])

        rows = beets.get_albums_by_release_ids([
            "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", "12856590",
        ])

        self.assertEqual(rows, [album])
        self.assertEqual(beets.get_albums_by_release_ids_calls, [[
            "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", "12856590",
        ]])

    def test_exact_album_projection_rejects_conflicting_numeric_identity(self) -> None:
        beets = FakeBeetsDB()
        beets.set_albums_by_artist("X", [{
            "id": 7,
            "album": "Conflicting pressing",
            "mb_albumid": "12856590",
            "discogs_albumid": "12856591",
        }])

        with self.assertRaisesRegex(
            ValueError,
            "conflicting numeric Discogs release identities",
        ):
            beets.get_albums_by_release_ids(["12856590"])

        self.assertEqual(
            beets.get_albums_by_release_ids(["99999999"]),
            [],
            "an unrelated conflicting row is outside the requested snapshot",
        )

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
        self.assertEqual(beets.get_tracks_by_mb_release_id("mbid-1"), [{
            "title": None, "track": None, "disc": None, "length": None,
            "format": None, "bitrate": None, "samplerate": None,
            "bitdepth": None,
        }])

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
        beets.set_album_ids_for_release("mbid-1", [1])
        beets.set_min_bitrate("mbid-1", 245)
        self.assertEqual(beets.get_min_bitrate("mbid-1"), 245)
        beets.set_album_ids_for_release("mbid-2", [2])
        beets.set_min_bitrate("mbid-2", 320)
        self.assertEqual(beets.get_min_bitrate("mbid-2"), 320)
        self.assertEqual(beets.get_min_bitrate_calls,
                         ["mbid-1", "mbid-2"])

    def test_get_min_bitrate_gates_on_presence_like_production(self) -> None:
        # Production resolves presence via locate first — an absent
        # release returns None no matter what; bitrate keys normalize.
        from lib.beets_db import ReleaseLocation

        beets = FakeBeetsDB()
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

    def test_album_and_items_absent_requires_both_absence_facts(self) -> None:
        beets = FakeBeetsDB()
        beets.set_album_detail(7, {"id": 7, "album": "A", "tracks": []})
        self.assertFalse(beets.album_and_items_absent(7))
        beets._album_detail.pop(7)
        beets.set_orphan_items_present(7)
        self.assertFalse(beets.album_and_items_absent(7))
        beets.set_orphan_items_present(7, False)
        self.assertTrue(beets.album_and_items_absent(7))

    def test_get_album_ids_by_mbids_derives_from_release_id_seeds(self) -> None:
        # Shares the set_album_ids_for_release seed store so presence
        # and album-id mapping can't disagree (the paired-consistency
        # concern from issue #121 the real _batch_lookup_album_ids
        # exists to solve). Multiple exact rows are ambiguous, never first-wins.
        beets = FakeBeetsDB()
        beets.set_album_ids_for_release("mbid-1", [17, 18])
        beets.set_album_ids_for_release("mbid-empty", [])
        out = beets.get_album_ids_by_mbids(["mbid-1", "mbid-empty", "mbid-2"])
        self.assertEqual(out, {})
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
            is_cbr=True,
            album_path="/Beets/Artist/Album",
        )
        beets.set_album_info("mbid-1", info)
        beets.set_item_paths("mbid-1", [
            (700 + index, f"/Beets/Moved/{index + 1:02d}.flac")
            for index in range(info.track_count)
        ])
        # Two-arg form (matches real signature: mb_release_id + cfg).
        current = beets.get_album_info("mbid-1", None)
        self.assertIsNot(current, info)
        self.assertEqual(current.album_path, "/Beets/Moved")

        beets.set_item_paths("mbid-1", [(700, "/Beets/Moved/01.flac")])
        narrowed = beets.get_album_info("mbid-1", None)
        self.assertIsNotNone(narrowed)
        assert narrowed is not None
        self.assertEqual(narrowed.track_count, 1)
        # Unseeded returns None.
        self.assertIsNone(beets.get_album_info("mbid-unknown"))
        self.assertEqual(
            beets.get_album_info_calls,
            ["mbid-1", "mbid-1", "mbid-unknown"],
        )

    def _rounding_boundary_album(self) -> AlbumInfo:
        """What the shrunk lineage world really reduces to.

        Hypothesis shrank to three tracks at 32 / 57 / 57 kbps. Their mean
        is 48666 bps, which production's ``kbps_from_bps`` rounds to 49
        while a floored copy reads 48 — the one-kbps gap that made a seeded
        AlbumInfo and its rebuilt twin disagree. (Seeding this AlbumInfo
        synthesizes 32 / 57 / 58, the whole-kilobit world with the same
        aggregates.)
        """
        return AlbumInfo(
            album_id=1,
            track_count=3,
            min_bitrate_kbps=32,
            avg_bitrate_kbps=49,
            median_bitrate_kbps=57,
            is_cbr=False,
            album_path="/Beets/Artist/Rounding",
            format="AAC",
        )

    def _sub_kilobit_tracks(self) -> list[dict[str, object]]:
        """Two items whose rate is not a whole number of kilobits.

        Real Beets rows carry raw bits per second, which almost never land
        on a kilobit boundary. 255600 is where flooring and rounding
        disagree — 255 against 256 — so a floored copy of either projection
        shows here and nowhere in the whole-kbps worlds ``set_album_info``
        can express.
        """
        return [
            {"bitrate": 255_600, "format": "MP3"},
            {"bitrate": 255_600, "format": "MP3"},
        ]

    def test_get_album_info_is_the_production_projection(self) -> None:
        """The fake must not re-derive what album_info_from_current derives.

        Its hand-copied projection floored bps->kbps after production moved
        to rounding, so the fake answered one kbps low for identical seeded
        items. Both sides of every assertion were the copy, so nothing could
        see it.
        """
        from lib.beets_db import CurrentBeetsUnique, album_info_from_current
        from lib.quality import QualityRankConfig
        from tests.fakes.beets import _lookup_identity

        beets = FakeBeetsDB()
        beets.set_tracks_for_release("mbid-sub", self._sub_kilobit_tracks())

        identity = _lookup_identity("mbid-sub")
        assert identity is not None
        resolution = beets.resolve_current_release(identity)
        assert isinstance(resolution, CurrentBeetsUnique)
        expected = album_info_from_current(
            resolution, QualityRankConfig.defaults(),
        )
        assert expected is not None
        actual = beets.get_album_info("mbid-sub")
        assert actual is not None

        self.assertEqual(actual.min_bitrate_kbps, expected.min_bitrate_kbps)
        self.assertEqual(actual.avg_bitrate_kbps, expected.avg_bitrate_kbps)
        self.assertEqual(
            actual.median_bitrate_kbps, expected.median_bitrate_kbps,
        )
        # 255 is what the floored copy reported.
        self.assertEqual(actual.min_bitrate_kbps, 256)
        self.assertEqual(actual.avg_bitrate_kbps, 256)
        self.assertEqual(actual.median_bitrate_kbps, 256)
        # Production also publishes the per-item codec set; the copy never
        # did, so a mixed-codec fake album silently looked single-codec.
        self.assertEqual(actual.formats_on_disk, expected.formats_on_disk)
        self.assertEqual(actual.formats_on_disk, frozenset({"mp3"}))

    def test_check_mbids_detail_shares_the_projection_reduction(self) -> None:
        """The two projections of one album must not disagree in the fake."""
        beets = FakeBeetsDB()
        beets.set_tracks_for_release("mbid-sub", self._sub_kilobit_tracks())

        detail = beets.check_mbids_detail(["mbid-sub"])["mbid-sub"]
        projected = beets.get_album_info("mbid-sub")
        assert projected is not None

        self.assertEqual(detail["beets_bitrate"], projected.min_bitrate_kbps)
        self.assertEqual(
            detail["beets_avg_bitrate"], projected.avg_bitrate_kbps,
        )
        self.assertEqual(detail["beets_bitrate"], 256)
        self.assertEqual(detail["beets_avg_bitrate"], 256)

    def test_seeding_refuses_a_world_production_cannot_reduce_to(self) -> None:
        """min/avg/median the synthesizer can no longer build.

        32 / 48 / 57 is what the floored helper derived from tracks at
        32 / 57 / 57, and it is not reachable from whole-kilobit tracks:
        the median pins two of them at 57000 bps, so the smallest mean the
        synthesizer can reach already rounds to 49. (Sub-kilobit tracks —
        31500 / 56500 / 56500 — do reduce to it, which is why the refusal
        is a limit of this constructor, not a claim about production.)
        """
        beets = FakeBeetsDB()
        with self.assertRaisesRegex(
            AssertionError, "not jointly expressible",
        ):
            beets.set_album_info("mbid-floored", AlbumInfo(
                album_id=1,
                track_count=3,
                min_bitrate_kbps=32,
                avg_bitrate_kbps=48,
                median_bitrate_kbps=57,
                is_cbr=False,
                album_path="/Beets/Artist/Floored",
                format="AAC",
            ))

    def test_seed_projection_checker_rejects_a_mismatched_album(self) -> None:
        """Known-bad self-test for the seed round-trip's mismatch clause.

        Driven through the real ``set_album_info``, so it also proves the
        check is wired into seeding rather than merely callable.
        """
        class _WrongSynthesis(FakeBeetsDB):
            @staticmethod
            def _synthesize_bitrates(info: AlbumInfo) -> list[int]:
                # Right min and median, wrong top track: production
                # averages this world to 53, not the 49 asked for. (Nudging
                # the top track by one kbps is NOT a mutant — 58000 still
                # reduces to 49, which is the whole point of this check.)
                return [32_000, 57_000, 70_000]

        beets = _WrongSynthesis()
        with self.assertRaisesRegex(
            AssertionError, "do not reduce to the requested AlbumInfo",
        ):
            beets.set_album_info("mbid-round", self._rounding_boundary_album())

    def test_seed_projection_checker_rejects_an_unresolvable_release(
        self,
    ) -> None:
        """Known-bad self-test for the seed round-trip's resolution clause.

        An album path that escapes the library root resolves ambiguous, so
        there is no projection to compare against — also through the real
        seeding path.
        """
        import dataclasses

        beets = FakeBeetsDB()
        with self.assertRaisesRegex(
            AssertionError, "does not resolve to a unique current release",
        ):
            beets.set_album_info("mbid-escape", dataclasses.replace(
                self._rounding_boundary_album(), album_path="../escape",
            ))

    def test_seeding_does_not_record_its_own_verification(self) -> None:
        """A seed is not an observation the test asked for."""
        beets = FakeBeetsDB()
        beets.set_album_info("mbid-round", self._rounding_boundary_album())

        self.assertEqual(beets.resolve_current_release_calls, [])
        self.assertEqual(beets.get_album_info_calls, [])

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


class TestFakeMergeRekeyForceClaimFence(unittest.TestCase):
    """The fake's force arm, term for term against the real SQL (#1080).

    ``update_request_release_for_merge``'s force arm is a five-term
    conjunction: a ``force_import`` job, ``running``, naming THIS request, on
    a row with no automation owner whose status is neither ``processing`` nor
    ``replaced``. Two permissiveness mutants — dropping ``job.status ==
    "running"`` and dropping ``job.request_id == request_id`` — survived the
    whole relevant suite before this table existed, while their real-SQL twins
    were killed by ``tests/test_pipeline_db.py::TestMergeRekeyUnderAForceClaim``
    on real PostgreSQL. A fake more permissive than the write it stands in for
    is exactly the test-fidelity Rule B failure: every seam test above it
    would agree with a production write that refuses.

    Every term is exercised on its own, from a world that otherwise rekeys.
    The one exception says so where it sits: the automation-owned case flips
    both owner terms at once, because migration 066's CHECK means PostgreSQL
    can only ever present them together. The ``processing`` status on its own
    is a fake-only world, and has its own case.
    """

    MERGED = "merged-id"
    SURVIVOR = "survivor-id"

    def _force_job(
        self,
        db: FakePipelineDB,
        *,
        request_id: int,
        download_log_id: int,
        claim: bool = True,
    ) -> int:
        from lib.import_queue import (
            IMPORT_JOB_FORCE,
            force_import_dedupe_key,
            force_import_payload,
        )

        job = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=request_id,
            dedupe_key=force_import_dedupe_key(download_log_id),
            payload=force_import_payload(
                download_log_id=download_log_id,
                failed_path="/quarantine/album",
            ),
        )
        db.mark_import_job_preview_importable(
            job.id, preview_result={}, message="ready",
        )
        if claim:
            claimed = db.claim_force_import_job_under_lock(
                job.id, request_id=request_id, worker_id="fence-test",
            )
            assert claimed is not None and claimed.status == "running"
        return job.id

    def _world(self, *, status: str = "wanted", claim: bool = True):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=41, mb_release_id=self.MERGED, status=status,
        ))
        job_id = self._force_job(
            db, request_id=41, download_log_id=1, claim=claim,
        )
        return db, job_id

    def _rekey(self, db: FakePipelineDB, job_id: int, request_id: int = 41):
        return db.update_request_release_for_merge(
            request_id,
            old_release_id=self.MERGED,
            new_release_id=self.SURVIVOR,
            expected_import_job_id=job_id,
        )

    def test_a_claimed_running_force_job_rekeys_its_own_request(self):
        db, job_id = self._world()

        self.assertTrue(self._rekey(db, job_id))

        row = db.request(41)
        assert row is not None
        self.assertEqual(row["mb_release_id"], self.SURVIVOR)
        # The lifecycle is untouched: force borrows the identity, never owns
        # the request.
        self.assertEqual(row["status"], "wanted")
        self.assertIsNone(row["active_automation_import_job_id"])

    def test_every_runnable_status_is_a_legal_force_rekey_target(self):
        for status in ("wanted", "imported", "unsearchable", "downloading"):
            with self.subTest(status=status):
                db, job_id = self._world(status=status)

                self.assertTrue(self._rekey(db, job_id))

                row = db.request(41)
                assert row is not None
                self.assertEqual(row["mb_release_id"], self.SURVIVOR)

    def test_a_job_that_is_not_running_writes_nothing(self):
        """``queued`` is not a claim, and neither is a finished job."""
        db, job_id = self._world(claim=False)
        job = db.get_import_job(job_id)
        assert job is not None and job.status != "running"

        self.assertFalse(self._rekey(db, job_id))

        row = db.request(41)
        assert row is not None
        self.assertEqual(row["mb_release_id"], self.MERGED)

        db, job_id = self._world()
        self.assertIsNotNone(db.mark_import_job_completed(
            job_id, result={}, message="done",
        ))

        self.assertFalse(self._rekey(db, job_id))

        row = db.request(41)
        assert row is not None
        self.assertEqual(row["mb_release_id"], self.MERGED)

    def test_a_job_naming_another_request_writes_nothing(self):
        db, _ = self._world()
        db.seed_request(make_request_row(id=42, mb_release_id="other-id"))
        other_job_id = self._force_job(db, request_id=42, download_log_id=2)

        # The claimed job is real and running — it just does not name request
        # 41, which is the term the mutant dropped.
        running = db.get_import_job(other_job_id)
        assert running is not None
        self.assertEqual(running.status, "running")
        self.assertFalse(self._rekey(db, other_job_id))

        row = db.request(41)
        assert row is not None
        self.assertEqual(row["mb_release_id"], self.MERGED)

    def test_a_non_force_job_writes_nothing(self):
        from lib.import_queue import IMPORT_JOB_YOUTUBE, youtube_import_payload

        db, _ = self._world()
        youtube = db.enqueue_import_job(
            IMPORT_JOB_YOUTUBE,
            request_id=41,
            payload=youtube_import_payload(
                staged_path="/Incoming/auto-import/album",
                request_id=41,
                browse_id="MPREb_x",
                download_log_id=9,
            ),
        )

        self.assertFalse(self._rekey(db, youtube.id))

        row = db.request(41)
        assert row is not None
        self.assertEqual(row["mb_release_id"], self.MERGED)

    def test_an_automation_owned_row_writes_nothing_under_a_force_claim(self):
        """Both owner terms at once — the world PostgreSQL can actually hold.

        Migration 066's owner-equivalence CHECK ties them together, so this
        is deliberately a two-term case. The ``processing`` term on its own is
        exercised by the next test, which the CHECK makes unreachable in
        PostgreSQL but perfectly reachable in the fake.
        """
        db, job_id = self._world()
        row = db.request(41)
        assert row is not None
        row["status"] = "processing"
        row["active_automation_import_job_id"] = 777

        self.assertFalse(self._rekey(db, job_id))

        after = db.request(41)
        assert after is not None
        self.assertEqual(after["mb_release_id"], self.MERGED)

    def test_a_processing_row_with_no_owner_writes_nothing(self):
        """The ``processing`` term alone, which only the fake can hold.

        The real SQL states ``status <> 'processing'`` and the owner-is-NULL
        term separately, and migration 066's CHECK means PostgreSQL can never
        present the first without the second — so in real PG the owner term
        already refuses this world and the status term is unreachable. The
        fake has no CHECK: it is the only place the status term can be
        exercised on its own, and every seam test in this repository runs
        against the fake. Without this case a fake force arm that admits
        ``processing`` agrees with a production write that refuses.
        """
        db, job_id = self._world()
        row = db.request(41)
        assert row is not None
        row["status"] = "processing"
        self.assertIsNone(row["active_automation_import_job_id"])

        self.assertFalse(self._rekey(db, job_id))

        after = db.request(41)
        assert after is not None
        self.assertEqual(after["mb_release_id"], self.MERGED)
        self.assertEqual(after["status"], "processing")

    def test_a_replaced_row_writes_nothing_under_a_force_claim(self):
        """Frozen audit ancestors are out of scope for BOTH claims."""
        db, job_id = self._world()
        row = db.request(41)
        assert row is not None
        row["status"] = "replaced"

        self.assertFalse(self._rekey(db, job_id))

        after = db.request(41)
        assert after is not None
        self.assertEqual(after["mb_release_id"], self.MERGED)

    def test_a_stale_identity_writes_nothing_under_a_force_claim(self):
        db, job_id = self._world()

        self.assertFalse(db.update_request_release_for_merge(
            41,
            old_release_id="somebody-elses-id",
            new_release_id=self.SURVIVOR,
            expected_import_job_id=job_id,
        ))

        row = db.request(41)
        assert row is not None
        self.assertEqual(row["mb_release_id"], self.MERGED)


class TestFakeMergeRekeyOperatorClaimFence(unittest.TestCase):
    """The fake's operator arm, term for term against the real SQL (#1089).

    ``update_request_release_for_merge``'s operator arm is a four-term
    conjunction: ``expected_import_job_id IS NULL``, ``status = 'imported'``,
    no automation owner attached, and no ``queued``/``running`` import job at
    all for this request (any job type — unlike the force arm's own
    ``EXISTS``, this ``NOT EXISTS`` carries no ``job_type`` filter). Every
    term is exercised on its own from a world that otherwise rekeys, mirroring
    ``TestFakeMergeRekeyForceClaimFence`` above: a fake more permissive than
    the write it stands in for is the test-fidelity Rule B failure this class
    exists to prevent.
    """

    MERGED = "merged-id"
    SURVIVOR = "survivor-id"

    def _world(self, *, status: str = "imported", owner: int | None = None):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=41,
            mb_release_id=self.MERGED,
            status=status,
            active_automation_import_job_id=owner,
        ))
        return db

    def _rekey(
        self,
        db: FakePipelineDB,
        *,
        request_id: int = 41,
        expected_import_job_id: int | None = None,
    ):
        return db.update_request_release_for_merge(
            request_id,
            old_release_id=self.MERGED,
            new_release_id=self.SURVIVOR,
            expected_import_job_id=expected_import_job_id,
        )

    def test_an_operator_call_rekeys_an_imported_unowned_unclaimed_row(self):
        db = self._world()

        self.assertTrue(self._rekey(db))

        row = db.request(41)
        assert row is not None
        self.assertEqual(row["mb_release_id"], self.SURVIVOR)
        self.assertEqual(row["status"], "imported")
        self.assertIsNone(row["active_automation_import_job_id"])
        self.assertEqual(
            db.update_request_release_for_merge_calls,
            [(41, self.MERGED, self.SURVIVOR, None)],
        )

    def test_a_real_job_id_never_satisfies_the_operator_arm(self):
        """``expected_import_job_id IS NULL`` — the arm-widening guard.

        A world that is otherwise exactly the operator's own (imported,
        unowned, nothing active) must still refuse a caller that supplies a
        real job id, even a job with no bearing on either claim arm (queued,
        not force). Dropping this guard would let the operator arm silently
        widen a force/automation caller's own — narrower — claim fence.
        """
        from lib.import_queue import IMPORT_JOB_FORCE, force_import_payload

        db = self._world()
        job = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=41,
            dedupe_key="force-41",
            payload=force_import_payload(
                download_log_id=1, failed_path="/quarantine/album",
            ),
        )
        db.mark_import_job_completed(job.id, result={}, message="done")

        self.assertFalse(self._rekey(db, expected_import_job_id=job.id))

        row = db.request(41)
        assert row is not None
        self.assertEqual(row["mb_release_id"], self.MERGED)

    def test_a_non_imported_status_writes_nothing_under_the_operator_arm(self):
        for status in (
            "wanted", "downloading", "unsearchable", "processing", "replaced",
        ):
            with self.subTest(status=status):
                db = self._world(status=status)

                self.assertFalse(self._rekey(db))

                row = db.request(41)
                assert row is not None
                self.assertEqual(row["mb_release_id"], self.MERGED)

    def test_an_automation_owned_imported_row_writes_nothing(self):
        """The owner term alone — reachable on ``imported`` only in the fake.

        Migration 066 ties the owner pointer to ``processing`` in real
        PostgreSQL, so this exact combination (``imported`` + an owner) never
        occurs there — but the fake has no CHECK, and it stands in for every
        seam test in this repository, so the term is exercised directly.
        """
        db = self._world(owner=777)

        self.assertFalse(self._rekey(db))

        row = db.request(41)
        assert row is not None
        self.assertEqual(row["mb_release_id"], self.MERGED)

    def test_a_queued_import_job_blocks_the_operator_arm(self):
        from lib.import_queue import IMPORT_JOB_YOUTUBE, youtube_import_payload

        db = self._world()
        db.enqueue_import_job(
            IMPORT_JOB_YOUTUBE,
            request_id=41,
            payload=youtube_import_payload(
                staged_path="/Incoming/auto-import/album",
                request_id=41,
                browse_id="MPREb_x",
                download_log_id=9,
            ),
        )

        self.assertFalse(self._rekey(db))

        row = db.request(41)
        assert row is not None
        self.assertEqual(row["mb_release_id"], self.MERGED)

    def test_a_running_import_job_blocks_the_operator_arm(self):
        """No ``job_type`` filter — an in-flight rescue blocks it too."""
        from lib.import_queue import IMPORT_JOB_YOUTUBE, youtube_import_payload

        db = self._world()
        job = db.enqueue_import_job(
            IMPORT_JOB_YOUTUBE,
            request_id=41,
            payload=youtube_import_payload(
                staged_path="/Incoming/auto-import/album",
                request_id=41,
                browse_id="MPREb_x",
                download_log_id=9,
            ),
        )
        db.mark_import_job_preview_importable(
            job.id, preview_result={}, message="ready",
        )
        claimed = db.claim_import_job_candidate(job.id, worker_id="fence-test")
        assert claimed is not None and claimed.status == "running"

        self.assertFalse(self._rekey(db))

        row = db.request(41)
        assert row is not None
        self.assertEqual(row["mb_release_id"], self.MERGED)

    def test_a_terminal_import_job_never_blocks_the_operator_arm(self):
        """Must-still-work: a completed/failed job on this request is inert."""
        from lib.import_queue import IMPORT_JOB_FORCE, force_import_payload

        for outcome in ("completed", "failed"):
            with self.subTest(outcome=outcome):
                db = self._world()
                job = db.enqueue_import_job(
                    IMPORT_JOB_FORCE,
                    request_id=41,
                    dedupe_key=f"force-41-{outcome}",
                    payload=force_import_payload(
                        download_log_id=1, failed_path="/quarantine/album",
                    ),
                )
                if outcome == "completed":
                    db.mark_import_job_completed(job.id, result={}, message="done")
                else:
                    db.mark_import_job_failed(job.id, error="synthetic failure")

                self.assertTrue(self._rekey(db))

                row = db.request(41)
                assert row is not None
                self.assertEqual(row["mb_release_id"], self.SURVIVOR)

    def test_an_active_job_on_a_different_request_never_blocks(self):
        from lib.import_queue import IMPORT_JOB_FORCE, force_import_payload

        db = self._world()
        db.seed_request(make_request_row(id=42, mb_release_id="other-id"))
        db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            dedupe_key="force-42",
            payload=force_import_payload(
                download_log_id=2, failed_path="/quarantine/other",
            ),
        )

        self.assertTrue(self._rekey(db))

        row = db.request(41)
        assert row is not None
        self.assertEqual(row["mb_release_id"], self.SURVIVOR)

    def test_a_stale_identity_writes_nothing_under_the_operator_arm(self):
        db = self._world()

        self.assertFalse(db.update_request_release_for_merge(
            41,
            old_release_id="somebody-elses-id",
            new_release_id=self.SURVIVOR,
            expected_import_job_id=None,
        ))

        row = db.request(41)
        assert row is not None
        self.assertEqual(row["mb_release_id"], self.MERGED)


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
            older_than=datetime.now(UTC) + timedelta(seconds=1))
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
            older_than=datetime.now(UTC) - timedelta(hours=1))
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
            older_than=datetime.now(UTC) + timedelta(seconds=1))
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
            datetime.now(UTC) - timedelta(days=10))

        removed = db.prune_search_ledger(
            deleted_before=datetime.now(UTC) - timedelta(days=7))

        self.assertEqual(removed, 1)
        self.assertNotIn("sid-old", db._search_ledger)
        self.assertIn("sid-recent", db._search_ledger)
        self.assertIn("sid-undeleted", db._search_ledger)


class TestFakePipelineDBTransferLedger(unittest.TestCase):
    """Self-tests for the slskd transfer write-ahead ownership ledger
    stubs (migration 045, issue #571)."""

    def test_record_transfer_enqueue_preserves_state(self):
        db = FakePipelineDB()
        db.record_transfer_enqueue([
            TransferLedgerRow(
                request_id=42, username="peer0", filename="Music\\a.flac",
                attempt_fingerprint="abcd1234"),
        ])
        rows = list(db._transfer_ledger.values())
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].request_id, 42)
        self.assertEqual(rows[0].username, "peer0")
        self.assertEqual(rows[0].filename, "Music\\a.flac")
        self.assertEqual(rows[0].attempt_fingerprint, "abcd1234")
        self.assertIsNone(rows[0].accepted_at)
        self.assertIsNone(rows[0].local_path)
        self.assertEqual(len(db.record_transfer_enqueue_calls), 1)

    def test_record_transfer_enqueue_empty_list_is_a_noop(self):
        db = FakePipelineDB()
        db.record_transfer_enqueue([])  # must not raise
        self.assertEqual(db._transfer_ledger, {})

    def test_record_transfer_enqueue_writes_one_row_per_file(self):
        db = FakePipelineDB()
        db.record_transfer_enqueue([
            TransferLedgerRow(request_id=1, username="p0", filename="a.flac"),
            TransferLedgerRow(request_id=1, username="p0", filename="b.flac"),
        ])
        self.assertEqual(len(db._transfer_ledger), 2)

    def test_stamp_transfer_completion_stamps_matching_row(self):
        db = FakePipelineDB()
        db.record_transfer_enqueue([
            TransferLedgerRow(request_id=1, username="p0", filename="a.flac"),
        ])
        db.confirm_transfer_enqueue("p0", "a.flac")
        stamped = db.stamp_transfer_completion(
            "p0", "a.flac", "/downloads/complete/a.flac")
        self.assertEqual(stamped, 1)
        row = next(iter(db._transfer_ledger.values()))
        self.assertEqual(row.local_path, "/downloads/complete/a.flac")
        self.assertIsNotNone(row.accepted_at)

    def test_confirm_transfer_enqueue_owns_newest_pending_row(self):
        db = FakePipelineDB()
        row = TransferLedgerRow(
            request_id=1, username="p0", filename="a.flac")
        db.record_transfer_enqueue([row, row])
        old_id = min(db._transfer_ledger)

        self.assertEqual(db.confirm_transfer_enqueue("p0", "a.flac"), 1)

        accepted = [
            item for item in db._transfer_ledger.values()
            if item.accepted_at is not None
        ]
        self.assertEqual(len(accepted), 1)
        self.assertNotEqual(accepted[0].id, old_id)
        self.assertEqual(db.get_owned_transfer_keys(), {("p0", "a.flac")})

    def test_stamp_transfer_completion_unledgered_pair_is_a_noop(self):
        db = FakePipelineDB()
        stamped = db.stamp_transfer_completion(
            "foreign-peer", "foreign.flac", "/downloads/x")
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
            datetime.now(UTC) - timedelta(minutes=10))
        db.record_transfer_enqueue([
            TransferLedgerRow(request_id=1, username="p0", filename="a.flac"),
        ])
        db.confirm_transfer_enqueue("p0", "a.flac")
        db.stamp_transfer_completion(
            "p0", "a.flac", "/downloads/complete/a.flac")
        rows = db._transfer_ledger.values()
        stamped_rows = [r for r in rows if r.local_path is not None]
        self.assertEqual(len(stamped_rows), 1)
        self.assertNotEqual(stamped_rows[0].id, old_id)

    def test_get_owned_local_paths_only_returns_stamped_rows(self):
        db = FakePipelineDB()
        db.record_transfer_enqueue([
            TransferLedgerRow(request_id=1, username="p0", filename="a.flac"),
            TransferLedgerRow(request_id=1, username="p0", filename="b.flac"),
        ])
        db.confirm_transfer_enqueue("p0", "a.flac")
        db.stamp_transfer_completion(
            "p0", "a.flac", "/downloads/a.flac")
        self.assertEqual(db.get_owned_local_paths(), {"/downloads/a.flac"})

    def test_get_abandoned_owned_local_paths_selects_only_wanted_without_state(self):
        from tests.helpers import make_request_row

        cases = [
            ("wanted, no state -> abandoned", "wanted", None, True),
            ("wanted, holding state", "wanted", {"files": []}, False),
            ("imported", "imported", None, False),
            ("processing", "processing", None, False),
            ("downloading", "downloading", None, False),
        ]
        for desc, status, state, expected in cases:
            with self.subTest(desc=desc):
                db = FakePipelineDB()
                db.seed_request(make_request_row(
                    id=1, status=status, active_download_state=state))
                db.record_transfer_enqueue([
                    TransferLedgerRow(
                        request_id=1, username="p0", filename="a.flac"),
                ])
                db.confirm_transfer_enqueue("p0", "a.flac")
                db.stamp_transfer_completion(
                    "p0", "a.flac", "/downloads/a.flac")

                paths = db.get_abandoned_owned_local_paths()

                self.assertEqual(
                    paths, {"/downloads/a.flac"} if expected else set())

    def test_get_abandoned_owned_local_paths_ignores_unstamped_rows(self):
        from tests.helpers import make_request_row

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=1, status="wanted", active_download_state=None))
        db.record_transfer_enqueue([
            TransferLedgerRow(request_id=1, username="p0", filename="a.flac"),
        ])
        db.confirm_transfer_enqueue("p0", "a.flac")

        self.assertEqual(db.get_abandoned_owned_local_paths(), set())

    def test_get_owned_transfer_keys_empty_before_any_record(self):
        self.assertEqual(FakePipelineDB().get_owned_transfer_keys(), set())

    def test_get_owned_transfer_keys_excludes_pending_intent(self):
        db = FakePipelineDB()
        db.record_transfer_enqueue([
            TransferLedgerRow(request_id=1, username="p0", filename="a.flac"),
            TransferLedgerRow(request_id=1, username="p0", filename="a.flac"),
            TransferLedgerRow(request_id=2, username="p1", filename="b.flac"),
        ])
        db.confirm_transfer_enqueue("p0", "a.flac")
        db.stamp_transfer_completion(
            "p0", "a.flac", "/downloads/a.flac")
        self.assertEqual(
            db.get_owned_transfer_keys(),
            {("p0", "a.flac")})

    def test_prune_transfer_ledger_keeps_accepted_active_request_rows(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="downloading"))
        db.record_transfer_enqueue([
            TransferLedgerRow(request_id=1, username="p0", filename="a.flac"),
        ])
        db.confirm_transfer_enqueue("p0", "a.flac")
        old_id = next(iter(db._transfer_ledger))
        db._transfer_ledger[old_id].enqueued_at = (
            datetime.now(UTC) - timedelta(days=200))

        removed = db.prune_transfer_ledger(
            older_than=datetime.now(UTC) - timedelta(days=90))

        self.assertEqual(removed, 0)
        self.assertIn(old_id, db._transfer_ledger)

    def test_prune_transfer_ledger_removes_pending_active_request_rows(self):
        db = FakePipelineDB()
        for request_id, status in ((1, "wanted"), (2, "downloading")):
            db.seed_request(make_request_row(id=request_id, status=status))
            db.record_transfer_enqueue([
                TransferLedgerRow(
                    request_id=request_id,
                    username=f"p{request_id}",
                    filename=f"{request_id}.flac",
                ),
            ])
            ledger_id = next(
                fake_id for fake_id, row in db._transfer_ledger.items()
                if row.request_id == request_id
            )
            db._transfer_ledger[ledger_id].enqueued_at = (
                datetime.now(UTC) - timedelta(days=200))

        removed = db.prune_transfer_ledger(
            older_than=datetime.now(UTC) - timedelta(days=90))

        self.assertEqual(removed, 2)
        self.assertEqual(db._transfer_ledger, {})

    def test_prune_transfer_ledger_removes_old_terminal_rows(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="imported"))
        db.record_transfer_enqueue([
            TransferLedgerRow(request_id=1, username="p0", filename="a.flac"),
        ])
        old_id = next(iter(db._transfer_ledger))
        db._transfer_ledger[old_id].enqueued_at = (
            datetime.now(UTC) - timedelta(days=200))

        removed = db.prune_transfer_ledger(
            older_than=datetime.now(UTC) - timedelta(days=90))

        self.assertEqual(removed, 1)
        self.assertNotIn(old_id, db._transfer_ledger)

    def test_prune_transfer_ledger_keeps_rows_inside_retention(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="imported"))
        db.record_transfer_enqueue([
            TransferLedgerRow(request_id=1, username="p0", filename="a.flac"),
        ])

        removed = db.prune_transfer_ledger(
            older_than=datetime.now(UTC) - timedelta(days=90))

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
            datetime.now(UTC) - timedelta(days=200))

        removed = db.prune_transfer_ledger(
            older_than=datetime.now(UTC) - timedelta(days=90))

        self.assertEqual(removed, 1)

    def _seed_accepted_row(
        self, db: FakePipelineDB, *, request_id: int, status: str,
        username: str, filename: str,
    ) -> None:
        from tests.helpers import make_request_row

        db.seed_request(make_request_row(id=request_id, status=status))
        db.record_transfer_enqueue([
            TransferLedgerRow(
                request_id=request_id, username=username, filename=filename),
        ])
        db.confirm_transfer_enqueue(username, filename)

    def test_get_conflicting_transfer_request_ids_empty_keys_is_a_noop(self):
        db = FakePipelineDB()
        self.assertEqual(
            db.get_conflicting_transfer_request_ids([], exclude_request_id=1),
            set(),
        )

    def test_get_conflicting_transfer_request_ids_downloading_owner_conflicts(self):
        db = FakePipelineDB()
        self._seed_accepted_row(
            db, request_id=99, status="downloading",
            username="p0", filename="a.flac")

        conflicting = db.get_conflicting_transfer_request_ids(
            [("p0", "a.flac")], exclude_request_id=1)

        self.assertEqual(conflicting, {99})

    def test_get_conflicting_transfer_request_ids_missing_fingerprint_key_blocks(
        self,
    ):
        """#1199 item 2 fake twin: an active_download_state that EXISTS
        but lacks "attempt_fingerprint" fails CLOSED unconditionally --
        both an old (30-day) and a current accepted row block, with no
        attempt-boundary rescue by age. Equivalence note: this replaces
        test_get_conflicting_transfer_request_ids_scopes_to_current_
        attempt, which asserted the OLD row did NOT block under the
        now-deleted deploy-window time-predicate fallback; that
        differentiation no longer exists in production."""
        from tests.helpers import make_request_row

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=99, status="downloading"))
        db.record_transfer_enqueue([
            TransferLedgerRow(request_id=99, username="OLD", filename="old.flac"),
        ])
        db.confirm_transfer_enqueue("OLD", "old.flac")
        old_id = next(
            fid for fid, r in db._transfer_ledger.items()
            if r.username == "OLD")
        db._transfer_ledger[old_id].enqueued_at = (
            datetime.now(UTC) - timedelta(days=30))

        db.request(99)["active_download_state"] = {
            "filetype": "flac", "enqueued_at": datetime.now(UTC).isoformat(),
            "files": [],
        }
        db.record_transfer_enqueue([
            TransferLedgerRow(request_id=99, username="NEW", filename="new.flac"),
        ])
        db.confirm_transfer_enqueue("NEW", "new.flac")

        self.assertEqual(
            db.get_conflicting_transfer_request_ids(
                [("OLD", "old.flac")], exclude_request_id=1),
            {99},
            "missing fingerprint key fails closed regardless of age",
        )
        self.assertEqual(
            db.get_conflicting_transfer_request_ids(
                [("NEW", "new.flac")], exclude_request_id=1),
            {99},
            "missing fingerprint key still blocks the current key too",
        )

    def test_get_conflicting_transfer_request_ids_null_state_fails_closed(self):
        """No active_download_state at all (never seeded) -- every
        accepted row for the 'downloading' owner still blocks."""
        db = FakePipelineDB()
        self._seed_accepted_row(
            db, request_id=99, status="downloading",
            username="p0", filename="a.flac")

        conflicting = db.get_conflicting_transfer_request_ids(
            [("p0", "a.flac")], exclude_request_id=1)

        self.assertEqual(conflicting, {99})

    def test_get_conflicting_transfer_request_ids_status_filter(self):
        # 'processing' included specifically to kill the
        # status-filter-widened mutant (#1178 PR2 review F1); every other
        # status here already happened to leave it unreachable.
        for status in ("wanted", "imported", "replaced", "processing"):
            with self.subTest(status=status):
                db = FakePipelineDB()
                self._seed_accepted_row(
                    db, request_id=99, status=status,
                    username="p0", filename="a.flac")

                conflicting = db.get_conflicting_transfer_request_ids(
                    [("p0", "a.flac")], exclude_request_id=1)

                self.assertEqual(conflicting, set())

    def test_get_conflicting_transfer_request_ids_excludes_own_rows(self):
        db = FakePipelineDB()
        self._seed_accepted_row(
            db, request_id=1, status="downloading",
            username="p0", filename="a.flac")

        conflicting = db.get_conflicting_transfer_request_ids(
            [("p0", "a.flac")], exclude_request_id=1)

        self.assertEqual(conflicting, set())

    def test_get_conflicting_transfer_request_ids_ignores_pending_intent(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=99, status="downloading"))
        db.record_transfer_enqueue([
            TransferLedgerRow(request_id=99, username="p0", filename="a.flac"),
        ])  # never confirmed -- accepted_at stays NULL

        conflicting = db.get_conflicting_transfer_request_ids(
            [("p0", "a.flac")], exclude_request_id=1)

        self.assertEqual(conflicting, set())

    def test_get_conflicting_transfer_request_ids_ignores_unrelated_keys(self):
        db = FakePipelineDB()
        self._seed_accepted_row(
            db, request_id=99, status="downloading",
            username="p0", filename="a.flac")

        conflicting = db.get_conflicting_transfer_request_ids(
            [("p0", "b.flac")], exclude_request_id=1)

        self.assertEqual(conflicting, set())

    def test_get_conflicting_transfer_request_ids_missing_request_row(self):
        db = FakePipelineDB()
        db.record_transfer_enqueue([
            TransferLedgerRow(request_id=99, username="p0", filename="a.flac"),
        ])
        db.confirm_transfer_enqueue("p0", "a.flac")

        conflicting = db.get_conflicting_transfer_request_ids(
            [("p0", "a.flac")], exclude_request_id=1)

        self.assertEqual(conflicting, set())


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


class TestFakePipelineDBSourceRejectAndRequeueGating(unittest.TestCase):
    """Issue #1077, R4-5 (round-4 review): ``FakePipelineDBSource.
    reject_and_requeue`` must gate identically to the real
    ``album_source.DatabaseSource.reject_and_requeue`` it stands in for —
    a single falsy ``request_id`` check before branching on
    ``import_job_id``, not a per-branch ``isinstance(request_id, int)``
    re-check that treats ``request_id=0`` as valid, and not an
    additional ``get_import_job(...) is not None`` requirement production
    never applies before taking the deferred path."""

    def _source(self):
        from tests.fakes import FakePipelineDB, FakePipelineDBSource
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="downloading"))
        return FakePipelineDBSource(db), db

    def test_falsy_request_id_writes_nothing_on_the_sync_branch(self) -> None:
        """``request_id=0`` is falsy — production's own ``if not
        request_id: return None`` (``album_source.py``) writes nothing for
        it. An ``isinstance(0, int)`` check would wrongly treat it as a
        valid request and write a full requeue+log+denylist."""
        from lib.quality import ValidationResult

        source, db = self._source()
        album = MagicMock(db_request_id=0)
        result = ValidationResult(
            valid=False, distance=0.4, scenario="high_distance",
            detail="test",
        )

        outcome = source.reject_and_requeue(album, result)

        self.assertIsNone(outcome)
        self.assertEqual(db.download_logs, [])
        self.assertEqual(db.denylist, [])

    def test_falsy_request_id_writes_nothing_on_the_deferred_branch(self) -> None:
        """Same falsy gate applies before the ``import_job_id`` branch
        decision is even made — ``request_id=0`` (falsy but ``isinstance``-
        valid, same distinguishing case as the sync-branch pin above) must
        not reach the deferred path either."""
        from lib.import_queue import IMPORT_JOB_FORCE, force_import_payload
        from lib.quality import ValidationResult

        source, db = self._source()
        job = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            payload=force_import_payload(
                download_log_id=1, failed_path="/tmp/cratedigger-r4-5-test"),
        )
        album = MagicMock(db_request_id=0)
        result = ValidationResult(
            valid=False, distance=0.4, scenario="high_distance",
            detail="test",
        )

        outcome = source.reject_and_requeue(
            album, result, import_job_id=job.id)

        self.assertIsNone(outcome)

    def test_unseeded_import_job_id_still_takes_the_deferred_path(self) -> None:
        """Production takes the deferred path on ``import_job_id is not
        None`` alone (``album_source.py``) — it never checks the job
        exists first. The fake used to require
        ``get_import_job(...) is not None``, which made an unseeded job id
        silently fall through to the SYNC branch instead — a materially
        different code path than production would take for the same
        input. This proves the fake now takes the SAME (deferred) path
        regardless of whether the id happens to be seeded."""
        from lib.quality import ValidationResult
        from lib.terminal_outcomes import PendingImportTerminalOutcome

        source, db = self._source()
        self.assertIsNone(db.get_import_job(999999))
        album = MagicMock(db_request_id=42)
        result = ValidationResult(
            valid=False, distance=0.4, scenario="high_distance",
            detail="test",
        )

        outcome = source.reject_and_requeue(
            album, result, import_job_id=999999)

        # The deferred path returns a PendingImportTerminalOutcome command
        # bundle, never a plain int/None sync-path return.
        self.assertIsInstance(outcome, PendingImportTerminalOutcome)
        # And critically: no download_log row was written directly — a
        # sync-branch fallthrough would have written one immediately.
        self.assertEqual(db.download_logs, [])


if __name__ == "__main__":
    unittest.main()
