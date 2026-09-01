"""Cross-cluster tests for the shared fakes and builders.

Since the #1313 split this module keeps what does not belong to one
cluster: the fake-to-production signature contract
(``TestPipelineDBFakeContract`` and its module-level diff helpers), the
two broad ``FakePipelineDB`` classes whose tests span several clusters,
the shared builders, and the ``FakePipelineDBSource`` gating tests.
A cluster's own self-tests live beside it in ``tests/test_fakes_<cluster>.py``,
which is also what ``scripts/targeted_test_selection.py`` derives from a
changed ``tests/fakes/pipeline_db/<cluster>.py`` or
``lib/pipeline_db/<cluster>.py``.
"""

import copy
import inspect
import unittest
from datetime import UTC, date, datetime, timedelta
from typing import Any, cast
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

import msgspec

from lib.grab_list import DownloadFile, GrabListEntry
from lib.pipeline_db import (
    PipelineDB,
    RequestSpectralStateUpdate,
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
from tests.dispatch_helpers import (
    claim_next_import_job,
    claim_next_import_preview_job,
    handoff_automation_owner,
)
from tests.evidence_helpers import make_album_quality_evidence
from tests.fakes import (
    FakeCursor,
    FakePipelineDB,
    RecordingProcessAlbum,
)
from tests.helpers import (
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

    def test_log_download_source_defaults_to_slskd(self):
        """Migration 037's ``source`` discriminator: ``log_download``'s
        default must match the production NOT NULL DEFAULT ``'slskd'`` so
        every existing caller (none of which pass ``source=``) keeps
        writing the same row shape it always has."""
        db = FakePipelineDB()
        db.log_download(42, outcome="success")

        self.assertEqual(db.download_logs[0].source, "slskd")

    def test_log_download_records_explicit_source(self):
        """Issue #1176 PR1: a future ``import-local`` caller passes
        ``source='local'`` — the fake must record exactly what it was
        given, not silently default or drop it. This is the fake-side
        half of the ``source`` parameter; the real-PG round trip lives in
        ``tests/test_pipeline_db.py``."""
        db = FakePipelineDB()
        db.log_download(42, outcome="success", source="local")

        self.assertEqual(db.download_logs[0].source, "local")

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

    def test_assert_log_failure_message_names_the_real_field(self):
        """Issue #1211 review F4 regression pin: assert_log's f-string used
        to interpolate ``{field}`` instead of ``{field_name}``. ``field``
        resolved to the module-scope ``dataclasses.field`` import the fake
        carried at the time, so every failure printed that function's repr
        instead of the column name. Assert the real field name appears in
        the message, not just that it raises."""
        db = FakePipelineDB()
        db.log_download(42, outcome="success", beets_distance=None)

        with self.assertRaisesRegex(AssertionError, r"beets_distance"):
            db.assert_log(self, 0, beets_distance=0.01)

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


# ---------------------------------------------------------------------------
# Triage cohort fakes (U15)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Persisted search plans (U1) — fake parity
# ---------------------------------------------------------------------------


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


class TestFakePipelineDBNewStubs(unittest.TestCase):
    """Self-tests for fake methods retroactively added under issue #140.

    These cover behaviour that tests relying on the fake may start
    exercising. Matches the New Work Checklist row in
    ``.claude/rules/code-quality.md``, which asks a new ``PipelineDB``
    method for an equivalent stub on ``FakePipelineDB`` plus a self-test
    in that cluster's own test module.
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

        with patch("tests.fakes.pipeline_db.dashboard._utcnow") as fake_now:
            fake_now.return_value = datetime(
                2026, 5, 9, 5, 0, tzinfo=UTC,
            )  # 2026-05-09 13:00 Perth
            resp = db.get_peer_metrics(days=14)

        by_date = {r["date"]: r for r in resp["days"]}
        self.assertEqual(by_date["2026-05-08"]["new_peers"], 1)
        self.assertEqual(by_date["2026-05-07"]["new_peers"], 0)
        self.assertEqual(by_date["2026-05-07"]["total_peers"], 0)
        self.assertEqual(by_date["2026-05-08"]["total_peers"], 1)


def _public_methods(cls: type) -> set[str]:
    """Return the set of non-underscore method names provided by ``cls``,
    including those contributed by base classes / mixins.

    BOTH classes are composed from cluster mixins now: ``PipelineDB`` from
    ``lib/pipeline_db/`` since #379, ``FakePipelineDB`` from
    ``tests/fakes/pipeline_db/`` since #1313. Neither keeps its public API
    in ``vars(cls)``: measured on the fake, ``vars`` yields 0 public
    callables where this MRO walk (skipping ``object``) yields 203.

    Degrading this to ``vars(cls)`` does NOT make the contract test below
    report the fake as missing, which is the intuitive but wrong
    prediction. It empties BOTH sides, so ``real - fake`` stays empty and
    the comparison passes vacuously. Measured: that mutant survived every
    test in ``TestPipelineDBFakeContract`` until
    ``test_recovered_surfaces_are_not_empty`` was added to catch it."""
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

    Models ``TestRouteContractAudit`` (tests/web/test_route_audit.py). The
    New Work Checklist row in ``.claude/rules/code-quality.md`` asks a new
    ``PipelineDB`` method for a matching stub on ``FakePipelineDB`` and a
    self-test in that cluster's test module; the stub half is enforced
    here at test time, not at review time.

    A new kwarg on a real method can otherwise be silently swallowed if the
    fake accepts ``**kwargs``.
    """

    def test_recovered_surfaces_are_not_empty(self) -> None:
        """``real - fake`` is empty when BOTH sides are empty.

        Both classes are mixin-composed now (``PipelineDB`` since #379,
        ``FakePipelineDB`` since #1313), so neither keeps a public method
        in ``vars(cls)``. Degrading ``_public_methods`` to a plain
        ``vars(cls)`` scan therefore leaves every other test in this class
        green while it compares two empty sets. Measured: that mutant
        survived all three tests here before this one existed.
        """
        for cls in (PipelineDB, FakePipelineDB):
            with self.subTest(cls=cls.__name__):
                self.assertGreater(
                    len(_public_methods(cls)), 100,
                    f"{cls.__name__} recovered a near-empty public surface, "
                    "so every comparison in this class is vacuous",
                )

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
