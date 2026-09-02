"""Self-tests for the FakePipelineDB album-quality-evidence cluster.

Split out of ``tests/test_fakes.py`` (#1313) so each cluster of the
fake has its sibling tests beside it.
"""
import unittest

import msgspec

from lib.quality import (
    AlbumQualityEvidenceFile,
    AudioQualityMeasurement,
    AudioToolDiagnostic,
    AudioValidationReport,
    legacy_unrecorded_audio_validation_report,
)
from tests.evidence_helpers import make_album_quality_evidence
from tests.fakes import (
    FakePipelineDB,
)
from tests.helpers import (
    make_request_row,
)


class TestFakeEvidenceRoundTrip(unittest.TestCase):
    """Content-addressed evidence rows round-trip and dedupe by key."""

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


class TestFakeEvidenceAddressing(unittest.TestCase):
    """The three addressing modes — download_log, import_job,
    request-current — the freeze on a replaced row, and the latest-candidate
    lookup that reads addressed rows back.
    """

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

    def test_request_current_evidence_refuses_a_replaced_row(self):
        """A ``replaced`` row is terminal audit and its current-evidence
        pointer is frozen (critical invariant 6). The fake's refusal had no
        test anywhere: deleting the status check left the whole suite green
        (#1313 review runner, E8).
        """
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="replaced"))
        db.seed_request(make_request_row(
            id=43, mb_release_id="mb-successor", status="wanted"))
        evidence = make_album_quality_evidence(mb_release_id="mb-replaced-freeze")
        db.upsert_album_quality_evidence(evidence)
        persisted = db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert persisted is not None and persisted.id is not None

        self.assertFalse(db.set_request_current_evidence(42, persisted.id))
        self.assertIsNone(db.get_request_current_evidence_id(42))
        # The successor is writable, so the refusal is the status and not a
        # blanket failure of the writer.
        self.assertTrue(db.set_request_current_evidence(43, persisted.id))
        self.assertEqual(db.get_request_current_evidence_id(43), persisted.id)

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


class TestFakeEvidenceValidation(unittest.TestCase):
    """Where the fake validates its input: it refuses malformed content keys,
    invalid snapshots and an inconsistent AAC lattice, and it accepts a
    zero-count empty fileset.
    """

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


class TestFakeEvidenceWritePolicy(unittest.TestCase):
    """Write policy: a weaker writer never overwrites a stronger stored
    tuple, the V0 claim fires once, and the attempt marker and
    enrichment gate only move forward.
    """

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

    def _stale_spectral_write(self, lineage_version: int) -> str | None:
        """Store graded evidence at one lineage, overwrite it with a writer
        carrying no grade, and report what the stored grade became."""
        db = FakePipelineDB()
        evidence = make_album_quality_evidence(
            mb_release_id=f"mb-lineage-{lineage_version}",
            lineage_version=lineage_version,
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
        db.upsert_album_quality_evidence(msgspec.structs.replace(
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
        ))
        loaded = db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert loaded is not None
        return loaded.measurement.spectral_grade

    def _stale_v0_write(self, lineage_version: int) -> object | None:
        """As above for the V0 tuple, which reads the same lineage guard."""
        from lib.quality import AlbumQualityV0Metric

        db = FakePipelineDB()
        evidence = make_album_quality_evidence(
            mb_release_id=f"mb-v0-lineage-{lineage_version}",
            lineage_version=lineage_version,
            v0_metric=AlbumQualityV0Metric(
                min_bitrate_kbps=201,
                avg_bitrate_kbps=259,
                median_bitrate_kbps=255,
                subject="installed",
                provenance="measured",
            ),
        )
        db.upsert_album_quality_evidence(evidence)
        db.upsert_album_quality_evidence(
            msgspec.structs.replace(evidence, v0_metric=None))
        loaded = db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert loaded is not None
        return loaded.v0_metric

    def test_both_preserve_guards_start_at_lineage_four(self):
        """Two guards read ``lineage_version >= 4`` — the spectral CASE and
        the V0 tuple — and every other test here takes the builder's default
        of 5, which passes ``> 4`` just as happily. Lineage 4 is the only
        value that tells the two comparisons apart (#1313 review runner, E2).
        """
        self.assertIsNone(self._stale_spectral_write(3))
        self.assertEqual(self._stale_spectral_write(4), "genuine")
        self.assertEqual(self._stale_spectral_write(5), "genuine")
        self.assertIsNone(self._stale_v0_write(3))
        self.assertIsNotNone(self._stale_v0_write(4))
        self.assertIsNotNone(self._stale_v0_write(5))

    def test_stored_source_path_wins_unless_it_is_blank(self):
        """A later writer cannot move the recorded source path, but a stored
        blank does not freeze the row — the guard is
        ``existing.source_path.strip()``, and only the blank case separates it
        from its negation (#1313 review runner, E5).
        """
        for stored, expected in (("/old/path", "/old/path"), ("   ", "/new/path")):
            with self.subTest(stored=stored):
                db = FakePipelineDB()
                evidence = make_album_quality_evidence(
                    mb_release_id="mb-source-path", source_path=stored)
                db.upsert_album_quality_evidence(evidence)
                db.upsert_album_quality_evidence(msgspec.structs.replace(
                    evidence, source_path="/new/path"))
                loaded = db.find_album_quality_evidence(
                    mb_release_id=evidence.mb_release_id,
                    snapshot_fingerprint=evidence.snapshot_fingerprint,
                )
                assert loaded is not None
                self.assertEqual(loaded.source_path, expected)


class TestFakeCandidateEvidenceBeetsChildRefusal(unittest.TestCase):
    """A live Beets child refuses the binding for every job type.

    ``set_import_job_candidate_evidence`` returns ``False`` above its SQL
    whenever the caller's lease carries a Beets child, and the
    statement's own non-automation arm never re-checks it. The fake put
    that clause inside its ``automation_import`` arm, so a force, local or
    YouTube job bound evidence mid-Beets-mutation. Same defect as the
    three in ``tests/test_fakes_import_jobs.py``; this is the one that
    lives in the evidence cluster.
    """

    def test_a_live_beets_child_refuses_a_non_automation_binding(self) -> None:
        from lib.import_execution import ExecutionLeaseSnapshot, ProcessIdentity
        from lib.import_queue import IMPORT_JOB_FORCE, force_import_payload

        def lease(beets: bool) -> ExecutionLeaseSnapshot:
            return ExecutionLeaseSnapshot(
                host_boot_id="boot-evidence-child",
                invocation_id="invocation-evidence-child",
                systemd_unit="cratedigger-import-preview-worker.service",
                worker=ProcessIdentity(pid=721, start_ticks=7021),
                beets=(
                    ProcessIdentity(pid=722, start_ticks=7022)
                    if beets else None
                ),
            )

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=4506, mb_release_id="mb-evidence-child", status="wanted",
        ))
        job = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=4506,
            payload=force_import_payload(
                download_log_id=4506, failed_path="/failed/evidence-child",
            ),
        )
        evidence = make_album_quality_evidence(
            mb_release_id="mb-evidence-child",
            source_path="/failed/evidence-child",
        )
        db.upsert_album_quality_evidence(evidence)
        persisted = db.find_album_quality_evidence(
            mb_release_id="mb-evidence-child",
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert persisted is not None and persisted.id is not None

        self.assertFalse(db.set_import_job_candidate_evidence(
            job.id, persisted.id, expected_execution_lease=lease(beets=True),
        ))
        refused = db.get_import_job(job.id)
        assert refused is not None
        self.assertIsNone(refused.candidate_evidence_id)

        # Must still work: no child, and the same call binds.
        self.assertTrue(db.set_import_job_candidate_evidence(
            job.id, persisted.id, expected_execution_lease=lease(beets=False),
        ))
        bound = db.get_import_job(job.id)
        assert bound is not None
        self.assertEqual(bound.candidate_evidence_id, persisted.id)


if __name__ == "__main__":
    unittest.main()
