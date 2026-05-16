"""Tests for evidence-only Wrong Matches cleanup."""

from __future__ import annotations

import os
import shutil
import tempfile
import types
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from lib.quality import (
    AlbumQualityEvidence,
    AlbumQualityEvidenceFile,
    AudioQualityMeasurement,
    QualityRankConfig,
)
from lib.quality_evidence import snapshot_fingerprint
from lib.wrong_match_cleanup_service import (
    OUTCOME_DELETE_FAILED,
    OUTCOME_DELETED,
    OUTCOME_KEPT_WOULD_IMPORT,
    OUTCOME_SKIPPED_ACTIVE_JOB,
    OUTCOME_SKIPPED_CANDIDATE_EVIDENCE_MISSING,
    OUTCOME_SKIPPED_CANDIDATE_EVIDENCE_STALE,
    OUTCOME_SKIPPED_CURRENT_EVIDENCE_MISSING,
    OUTCOME_SKIPPED_CURRENT_EVIDENCE_STALE,
    OUTCOME_SKIPPED_MISSING_PATH,
    OUTCOME_SKIPPED_OPERATIONAL,
    cleanup_all_wrong_matches,
    cleanup_wrong_match,
)
from tests.fakes import FakePipelineDB
from tests.helpers import make_request_row


def _cfg() -> types.SimpleNamespace:
    return types.SimpleNamespace(
        quality_ranks=QualityRankConfig.defaults(),
        verified_lossless_target="",
    )


def _make_source(root: str, name: str) -> str:
    source = os.path.join(root, name)
    os.mkdir(source)
    with open(os.path.join(source, "01.mp3"), "wb") as handle:
        handle.write(b"audio")
    return source


def _evidence_files(source: str) -> list[AlbumQualityEvidenceFile]:
    full = os.path.join(source, "01.mp3")
    stat = os.stat(full)
    return [
        AlbumQualityEvidenceFile(
            relative_path="01.mp3",
            size_bytes=int(stat.st_size),
            mtime_ns=int(stat.st_mtime_ns),
            extension="mp3",
            container="mp3",
            codec="mp3",
        )
    ]


def _evidence(
    source: str,
    *,
    mb_release_id: str = "mbid-1",
    audio_corrupt: bool = False,
) -> AlbumQualityEvidence:
    files = _evidence_files(source)
    return AlbumQualityEvidence(
        mb_release_id=mb_release_id,
        snapshot_fingerprint=snapshot_fingerprint(files),
        source_path=source,
        measurement=AudioQualityMeasurement(
            min_bitrate_kbps=245,
            avg_bitrate_kbps=256,
            median_bitrate_kbps=252,
            format="mp3 v0",
            spectral_grade="genuine",
        ),
        measured_at=datetime(2026, 5, 1, tzinfo=timezone.utc),
        files=files,
        codec="mp3",
        container="mp3",
        storage_format="mp3 v0",
        audio_corrupt=audio_corrupt,
        audio_file_count=len(files),
        filetype_band="mp3",
        folder_layout="flat",
    )


def _store_evidence(
    db: FakePipelineDB,
    evidence: AlbumQualityEvidence,
) -> int:
    db.upsert_album_quality_evidence(evidence)
    stored = db.find_album_quality_evidence(
        mb_release_id=evidence.mb_release_id,
        snapshot_fingerprint=evidence.snapshot_fingerprint,
    )
    assert stored is not None and stored.id is not None
    return stored.id


def _log_wrong_match(
    db: FakePipelineDB,
    request_id: int,
    source: str,
) -> int:
    return db.log_download(
        request_id,
        outcome="rejected",
        validation_result={
            "scenario": "wrong_match",
            "failed_path": source,
        },
    )


class WrongMatchCleanupServiceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.mkdtemp()
        self.db = FakePipelineDB()
        self.db.seed_request(make_request_row(
            id=1,
            status="wanted",
            mb_release_id="mbid-1",
        ))

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_missing_download_log_fk_does_not_use_sibling_import_job_evidence(self) -> None:
        source = _make_source(self.tmp, "sparse-source")
        log_id = _log_wrong_match(self.db, 1, source)

        job = self.db.enqueue_import_job(
            "automation_import",
            request_id=1,
            payload={},
        )
        evidence_id = _store_evidence(
            self.db,
            _evidence(source, audio_corrupt=True),
        )
        self.db.set_import_job_candidate_evidence(job.id, evidence_id)
        self.db.mark_import_job_completed(job.id)

        result = cleanup_wrong_match(self.db, log_id, cfg=_cfg())

        self.assertEqual(result.outcome, OUTCOME_SKIPPED_CANDIDATE_EVIDENCE_MISSING)
        self.assertTrue(os.path.isdir(source))
        vr = self.db.download_logs[-1].validation_result
        self.assertIn("failed_path", vr)

    def test_bulk_summary_deletes_only_confident_rejects(self) -> None:
        delete_source = _make_source(self.tmp, "delete-source")
        keep_source = _make_source(self.tmp, "keep-source")
        stale_source = _make_source(self.tmp, "stale-source")

        delete_id = _log_wrong_match(self.db, 1, delete_source)
        self.db.set_download_log_candidate_evidence(
            delete_id,
            _store_evidence(
                self.db,
                _evidence(
                    delete_source,
                    mb_release_id="mbid-delete",
                    audio_corrupt=True,
                ),
            ),
        )

        keep_id = _log_wrong_match(self.db, 1, keep_source)
        self.db.set_download_log_candidate_evidence(
            keep_id,
            _store_evidence(
                self.db,
                _evidence(keep_source, mb_release_id="mbid-keep"),
            ),
        )

        stale_id = _log_wrong_match(self.db, 1, stale_source)
        self.db.set_download_log_candidate_evidence(
            stale_id,
            _store_evidence(self.db, _evidence(stale_source, mb_release_id="mbid-stale")),
        )
        with open(os.path.join(stale_source, "02.mp3"), "wb") as handle:
            handle.write(b"changed")

        missing_source = _make_source(self.tmp, "missing-evidence")
        _log_wrong_match(self.db, 1, missing_source)

        summary = cleanup_all_wrong_matches(
            self.db,
            confirm_all_wrong_matches=True,
            cfg=_cfg(),
        )

        self.assertEqual(summary.deleted, 1)
        self.assertEqual(summary.kept_would_import, 1)
        self.assertEqual(summary.skipped_candidate_evidence_stale, 1)
        self.assertEqual(summary.skipped_candidate_evidence_missing, 1)
        self.assertFalse(os.path.exists(delete_source))
        self.assertTrue(os.path.isdir(keep_source))
        self.assertTrue(os.path.isdir(stale_source))
        self.assertTrue(os.path.isdir(missing_source))

    def test_active_matching_import_job_skips_before_delete(self) -> None:
        source = _make_source(self.tmp, "active-source")
        log_id = _log_wrong_match(self.db, 1, source)
        self.db.set_download_log_candidate_evidence(
            log_id,
            _store_evidence(self.db, _evidence(source, audio_corrupt=True)),
        )
        self.db.enqueue_import_job(
            "force_import",
            request_id=1,
            payload={"download_log_id": log_id, "failed_path": source},
        )

        result = cleanup_wrong_match(self.db, log_id, cfg=_cfg())

        self.assertEqual(result.outcome, OUTCOME_SKIPPED_ACTIVE_JOB)
        self.assertTrue(os.path.isdir(source))

    def test_authorized_delete_clears_same_request_exact_path_duplicates_only(self) -> None:
        source = _make_source(self.tmp, "duplicate-source")
        other_source = _make_source(self.tmp, "other-source")

        older_id = _log_wrong_match(self.db, 1, source)
        newest_id = _log_wrong_match(self.db, 1, source)
        other_path_id = _log_wrong_match(self.db, 1, other_source)
        self.db.seed_request(make_request_row(
            id=2,
            status="wanted",
            mb_release_id="mbid-2",
        ))
        other_request_id = _log_wrong_match(self.db, 2, source)

        self.db.set_download_log_candidate_evidence(
            newest_id,
            _store_evidence(self.db, _evidence(source, audio_corrupt=True)),
        )

        result = cleanup_wrong_match(self.db, newest_id, cfg=_cfg())

        self.assertEqual(result.outcome, OUTCOME_DELETED)
        self.assertEqual(result.cleared_rows, 2)

        by_id = {row.id: row for row in self.db.download_logs}
        self.assertNotIn("failed_path", by_id[older_id].validation_result)
        self.assertNotIn("failed_path", by_id[newest_id].validation_result)
        self.assertIn("failed_path", by_id[other_path_id].validation_result)
        self.assertIn("failed_path", by_id[other_request_id].validation_result)

    def test_lock_contention_is_reported_as_active_job_skip(self) -> None:
        source = _make_source(self.tmp, "locked-source")
        log_id = _log_wrong_match(self.db, 1, source)
        self.db.set_download_log_candidate_evidence(
            log_id,
            _store_evidence(self.db, _evidence(source, audio_corrupt=True)),
        )
        self.db.set_advisory_lock_result(False)

        result = cleanup_wrong_match(self.db, log_id, cfg=_cfg())

        self.assertEqual(result.outcome, OUTCOME_SKIPPED_ACTIVE_JOB)
        self.assertTrue(os.path.isdir(source))

    def test_active_job_that_appears_under_lock_skips_delete(self) -> None:
        source = _make_source(self.tmp, "under-lock-active-source")
        log_id = _log_wrong_match(self.db, 1, source)
        self.db.set_download_log_candidate_evidence(
            log_id,
            _store_evidence(self.db, _evidence(source, audio_corrupt=True)),
        )
        job = self.db.enqueue_import_job(
            "force_import",
            request_id=1,
            payload={"download_log_id": log_id, "failed_path": source},
        )
        calls = 0

        def active_jobs(**_kwargs):
            nonlocal calls
            calls += 1
            return [] if calls == 1 else [job]

        self.db.list_active_import_jobs_for_wrong_match = active_jobs
        with patch(
            "lib.wrong_match_cleanup_service.cleanup_wrong_match_source",
        ) as cleanup_source:
            result = cleanup_wrong_match(self.db, log_id, cfg=_cfg())

        self.assertEqual(result.outcome, OUTCOME_SKIPPED_ACTIVE_JOB)
        self.assertEqual(calls, 2)
        cleanup_source.assert_not_called()
        self.assertTrue(os.path.isdir(source))

    def test_imported_request_without_current_evidence_keeps_row(self) -> None:
        source = _make_source(self.tmp, "missing-current-source")
        log_id = _log_wrong_match(self.db, 1, source)
        self.db.set_download_log_candidate_evidence(
            log_id,
            _store_evidence(self.db, _evidence(source, audio_corrupt=True)),
        )
        request = self.db.request(1)
        request["status"] = "imported"
        request["imported_path"] = _make_source(self.tmp, "current-source")

        summary = cleanup_all_wrong_matches(
            self.db,
            confirm_all_wrong_matches=True,
            cfg=_cfg(),
        )

        self.assertEqual(summary.skipped_current_evidence_missing, 1)
        self.assertEqual(summary.results[0].outcome, OUTCOME_SKIPPED_CURRENT_EVIDENCE_MISSING)
        self.assertTrue(os.path.isdir(source))
        self.assertIn("failed_path", self.db.download_logs[-1].validation_result)

    def test_imported_request_with_missing_current_evidence_row_keeps_row(self) -> None:
        source = _make_source(self.tmp, "missing-current-row-source")
        log_id = _log_wrong_match(self.db, 1, source)
        self.db.set_download_log_candidate_evidence(
            log_id,
            _store_evidence(self.db, _evidence(source, audio_corrupt=True)),
        )
        request = self.db.request(1)
        request["status"] = "imported"
        request["imported_path"] = _make_source(self.tmp, "current-row-source")
        self.db.set_request_current_evidence(1, 99999)

        result = cleanup_wrong_match(self.db, log_id, cfg=_cfg())

        self.assertEqual(result.outcome, OUTCOME_SKIPPED_CURRENT_EVIDENCE_MISSING)
        self.assertTrue(os.path.isdir(source))
        self.assertIn("failed_path", self.db.download_logs[-1].validation_result)

    def test_imported_request_with_stale_current_evidence_keeps_row(self) -> None:
        source = _make_source(self.tmp, "stale-current-source")
        current = _make_source(self.tmp, "current-stale-source")
        log_id = _log_wrong_match(self.db, 1, source)
        self.db.set_download_log_candidate_evidence(
            log_id,
            _store_evidence(self.db, _evidence(source, audio_corrupt=True)),
        )
        current_evidence_id = _store_evidence(
            self.db,
            _evidence(current, mb_release_id="current-mbid"),
        )
        with open(os.path.join(current, "02.mp3"), "wb") as handle:
            handle.write(b"changed")
        request = self.db.request(1)
        request["status"] = "imported"
        request["imported_path"] = current
        self.db.set_request_current_evidence(1, current_evidence_id)

        result = cleanup_wrong_match(self.db, log_id, cfg=_cfg())

        self.assertEqual(result.outcome, OUTCOME_SKIPPED_CURRENT_EVIDENCE_STALE)
        self.assertTrue(os.path.isdir(source))
        self.assertIn("failed_path", self.db.download_logs[-1].validation_result)

    def test_delete_failure_is_reported_at_service_layer(self) -> None:
        source = _make_source(self.tmp, "delete-failure-source")
        log_id = _log_wrong_match(self.db, 1, source)
        self.db.set_download_log_candidate_evidence(
            log_id,
            _store_evidence(self.db, _evidence(source, audio_corrupt=True)),
        )
        cleanup = types.SimpleNamespace(
            success=False,
            error="permission denied",
            path_missing=False,
            cleared_rows=0,
            deleted_path=None,
        )

        with patch(
            "lib.wrong_match_cleanup_service.cleanup_wrong_match_source",
            return_value=cleanup,
        ):
            summary = cleanup_all_wrong_matches(
                self.db,
                confirm_all_wrong_matches=True,
                cfg=_cfg(),
            )

        self.assertEqual(summary.delete_failed, 1)
        self.assertEqual(summary.results[0].outcome, OUTCOME_DELETE_FAILED)
        self.assertTrue(os.path.isdir(source))
        self.assertIn("failed_path", self.db.download_logs[-1].validation_result)

    def test_delete_race_is_reported_at_service_layer_without_clearing(self) -> None:
        source = _make_source(self.tmp, "delete-race-source")
        log_id = _log_wrong_match(self.db, 1, source)
        self.db.set_download_log_candidate_evidence(
            log_id,
            _store_evidence(self.db, _evidence(source, audio_corrupt=True)),
        )
        cleanup = types.SimpleNamespace(
            success=True,
            error=None,
            path_missing=True,
            cleared_rows=0,
            deleted_path=None,
        )

        with patch(
            "lib.wrong_match_cleanup_service.cleanup_wrong_match_source",
            return_value=cleanup,
        ):
            summary = cleanup_all_wrong_matches(
                self.db,
                confirm_all_wrong_matches=True,
                cfg=_cfg(),
            )

        self.assertEqual(summary.skipped_missing_path, 1)
        self.assertEqual(summary.results[0].outcome, OUTCOME_SKIPPED_MISSING_PATH)
        self.assertIn("failed_path", self.db.download_logs[-1].validation_result)

    def test_operational_failure_is_counted_at_service_layer(self) -> None:
        source = _make_source(self.tmp, "operational-source")
        log_id = _log_wrong_match(self.db, 1, source)
        self.db.set_download_log_candidate_evidence(
            log_id,
            _store_evidence(self.db, _evidence(source, audio_corrupt=True)),
        )

        def raise_get_entry(_download_log_id: int):
            raise RuntimeError("db unavailable")

        self.db.get_download_log_entry = raise_get_entry

        summary = cleanup_all_wrong_matches(
            self.db,
            confirm_all_wrong_matches=True,
            cfg=_cfg(),
        )

        self.assertEqual(summary.skipped_operational, 1)
        self.assertEqual(summary.results[0].outcome, OUTCOME_SKIPPED_OPERATIONAL)
        self.assertTrue(os.path.isdir(source))

    def test_bulk_requires_explicit_confirmation(self) -> None:
        with self.assertRaisesRegex(ValueError, "confirm_all_wrong_matches"):
            cleanup_all_wrong_matches(self.db, cfg=_cfg())


if __name__ == "__main__":
    unittest.main()
