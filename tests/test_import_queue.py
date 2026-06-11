"""Tests for the shared import queue worker."""

import json
import os
import shutil
import tempfile
import threading
import time
import unittest
from datetime import datetime, timedelta, timezone
from typing import Any, cast
from unittest.mock import MagicMock, patch

from lib.config import CratediggerConfig
from lib.import_dispatch import (
    DISPATCH_CODE_QUALITY_PIPELINE_REJECTED,
    DispatchOutcome,
)
from lib.import_queue import (
    IMPORT_JOB_AUTOMATION,
    IMPORT_JOB_FORCE,
    IMPORT_JOB_MANUAL,
    IMPORT_JOB_YOUTUBE,
    automation_import_dedupe_key,
    force_import_dedupe_key,
    force_import_payload,
    manual_import_dedupe_key,
    manual_import_payload,
)
from lib.import_preview import ImportPreviewResult
from lib.quality import (
    AudioQualityMeasurement,
    ImportResult,
    ValidationResult,
)
from lib.quality_evidence import snapshot_audio_files
from lib.staged_album import StagedAlbum
from tests.fakes import FakeBeetsDB, FakePipelineDB
from tests.helpers import (
    make_album_quality_evidence,
    make_ctx_with_fake_db,
    make_download_file,
    make_grab_list_entry,
    make_request_row,
)


# Migration 021 helpers — seed evidence and wire the FK chain that
# production reads through.
def _seed_candidate_for_download_log(db, log_id: int, *, mb_release_id: str,
                                     **kwargs):
    evidence = make_album_quality_evidence(mb_release_id=mb_release_id, **kwargs)
    db.upsert_album_quality_evidence(evidence)
    persisted = db.find_album_quality_evidence(
        mb_release_id=evidence.mb_release_id,
        snapshot_fingerprint=evidence.snapshot_fingerprint,
    )
    assert persisted is not None and persisted.id is not None
    db.set_download_log_candidate_evidence(log_id, persisted.id)
    return persisted


def _seed_candidate_for_import_job(db, job_id: int, *, mb_release_id: str,
                                   **kwargs):
    evidence = make_album_quality_evidence(mb_release_id=mb_release_id, **kwargs)
    db.upsert_album_quality_evidence(evidence)
    persisted = db.find_album_quality_evidence(
        mb_release_id=evidence.mb_release_id,
        snapshot_fingerprint=evidence.snapshot_fingerprint,
    )
    assert persisted is not None and persisted.id is not None
    db.set_import_job_candidate_evidence(job_id, persisted.id)
    return persisted


def _seed_current_for_request(db, request_id: int, *, mb_release_id: str,
                              **kwargs):
    evidence = make_album_quality_evidence(mb_release_id=mb_release_id, **kwargs)
    db.upsert_album_quality_evidence(evidence)
    persisted = db.find_album_quality_evidence(
        mb_release_id=evidence.mb_release_id,
        snapshot_fingerprint=evidence.snapshot_fingerprint,
    )
    assert persisted is not None and persisted.id is not None
    db.set_request_current_evidence(request_id, persisted.id)
    return persisted


def _make_failed_import_source() -> tuple[str, str]:
    root = tempfile.mkdtemp()
    source = os.path.join(root, "failed_imports", "Album")
    os.makedirs(source)
    return root, source


class TestAutomationEvidenceReuse(unittest.TestCase):
    def test_previewed_automation_job_skips_preimport_gates(self):
        from lib.download import _process_beets_validation
        from lib.quality import ValidationResult

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            mb_release_id="mbid-123",
            status="downloading",
        ))
        job = db.enqueue_import_job(
            IMPORT_JOB_AUTOMATION,
            request_id=42,
            dedupe_key=automation_import_dedupe_key(42),
            payload={},
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            with open(os.path.join(tmpdir, "01 - Track.mp3"), "wb") as handle:
                handle.write(b"audio")
            _seed_candidate_for_import_job(
                db, job.id,
                mb_release_id="mbid-123",
                files=snapshot_audio_files(tmpdir),
                measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=245,
                    avg_bitrate_kbps=256,
                    median_bitrate_kbps=252,
                    format="MP3 V0",
                    spectral_grade="genuine",
                ),
                codec="mp3",
                container="mp3",
                storage_format="mp3 v0",
            )
            cfg = CratediggerConfig(
                beets_harness_path="/nix/store/fake/harness/run_beets_harness.sh",
                beets_distance_threshold=0.15,
                beets_staging_dir=os.path.join(tmpdir, "staging"),
                slskd_download_dir=tmpdir,
                pipeline_db_enabled=True,
            )
            ctx = make_ctx_with_fake_db(db, cfg=cfg)
            album_data = make_grab_list_entry(
                album_id=42,
                artist="Artist",
                title="Album",
                mb_release_id="mbid-123",
                db_source="request",
                db_request_id=42,
            )
            staged_album = StagedAlbum(current_path=tmpdir, request_id=42)

            with patch("lib.beets.beets_validate", return_value=ValidationResult(
                valid=True,
                distance=0.05,
                scenario="strong_match",
            )), \
                 patch(
                     "lib.download._handle_valid_result",
                     return_value=DispatchOutcome(True, "imported"),
                 ) as handle_valid:
                result = _process_beets_validation(
                    album_data,
                    staged_album,
                    ctx,
                    import_job_id=job.id,
                )

        assert result is not None
        self.assertTrue(result.success)
        handle_valid.assert_called_once()
        self.assertEqual(handle_valid.call_args.kwargs["import_job_id"], job.id)

    def test_stale_previewed_automation_evidence_fails_before_preimport(self):
        from lib.download import _process_beets_validation
        from lib.quality import ValidationResult

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            mb_release_id="mbid-123",
            status="downloading",
        ))
        job = db.enqueue_import_job(
            IMPORT_JOB_AUTOMATION,
            request_id=42,
            dedupe_key=automation_import_dedupe_key(42),
            payload={},
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            track = os.path.join(tmpdir, "01 - Track.mp3")
            with open(track, "wb") as handle:
                handle.write(b"audio")
            _seed_candidate_for_import_job(
                db, job.id,
                mb_release_id="mbid-123",
                files=snapshot_audio_files(tmpdir),
                measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=245,
                    avg_bitrate_kbps=256,
                    median_bitrate_kbps=252,
                    format="MP3 V0",
                    spectral_grade="genuine",
                ),
                codec="mp3",
                container="mp3",
                storage_format="mp3 v0",
            )
            with open(track, "ab") as handle:
                handle.write(b" changed")
            cfg = CratediggerConfig(
                beets_harness_path="/nix/store/fake/harness/run_beets_harness.sh",
                beets_distance_threshold=0.15,
                beets_staging_dir=os.path.join(tmpdir, "staging"),
                slskd_download_dir=tmpdir,
                pipeline_db_enabled=True,
            )
            ctx = make_ctx_with_fake_db(db, cfg=cfg)
            album_data = make_grab_list_entry(
                album_id=42,
                artist="Artist",
                title="Album",
                mb_release_id="mbid-123",
                db_source="request",
                db_request_id=42,
            )
            staged_album = StagedAlbum(current_path=tmpdir, request_id=42)

            handle_valid_calls: list[tuple] = []

            def _record_handle_valid(*args, **kwargs):
                handle_valid_calls.append((args, kwargs))
                return None

            with patch("lib.beets.beets_validate", return_value=ValidationResult(
                valid=True,
                distance=0.05,
                scenario="strong_match",
            )):
                result = _process_beets_validation(
                    album_data,
                    staged_album,
                    ctx,
                    import_job_id=job.id,
                    handle_valid_fn=_record_handle_valid,
                )

        assert result is not None
        self.assertFalse(result.success)
        self.assertIn("Candidate quality evidence unavailable", result.message)
        self.assertEqual(handle_valid_calls, [])


class TestImporterWorker(unittest.TestCase):
    def _patch_beets_album(self, album_path: str, *, min_bitrate: int):
        from lib.beets_db import AlbumInfo

        beets = FakeBeetsDB()
        info = AlbumInfo(
            album_id=1,
            track_count=1,
            min_bitrate_kbps=min_bitrate,
            avg_bitrate_kbps=min_bitrate,
            median_bitrate_kbps=min_bitrate,
            is_cbr=False,
            album_path=album_path,
            format="MP3",
        )
        # Seed the same row for any release_id the test queries — the
        # importer worker uses the request's mb_release_id, which the
        # tests don't pin to a specific value.
        beets._album_info_default = info
        return patch("lib.beets_db.BeetsDB", return_value=beets)

    def _mark_importable(
        self,
        db: FakePipelineDB,
        job,
        *,
        preview_result: dict[str, Any] | None = None,
    ):
        updated = db.mark_import_job_preview_importable(
            job.id,
            preview_result=preview_result or {"verdict": "would_import"},
            message="ready",
        )
        assert updated is not None
        return updated

    def _result(self, job: Any) -> dict[str, Any]:
        assert job.result is not None
        return job.result

    def _log_wrong_match(
        self,
        db: FakePipelineDB,
        *,
        request_id: int = 42,
        failed_path: str,
        username: str = "alice",
    ) -> int:
        db.log_download(
            request_id,
            soulseek_username=username,
            outcome="rejected",
            validation_result={
                "scenario": "high_distance",
                "failed_path": failed_path,
            },
        )
        return db.download_logs[-1].id

    def _cleanup_preview(
        self,
        log_id: int,
        *,
        verdict: str = "confident_reject",
        cleanup_eligible: bool = True,
        reason: str = "fresh cleanup-safe reject",
    ) -> ImportPreviewResult:
        return ImportPreviewResult(
            mode="download_log",
            verdict=verdict,
            would_import=verdict == "would_import",
            confident_reject=verdict == "confident_reject",
            uncertain=verdict == "uncertain",
            cleanup_eligible=cleanup_eligible,
            decision=reason,
            reason=reason,
            download_log_id=log_id,
        )

    def _seed_cleanup_reject_evidence(
        self,
        db: FakePipelineDB,
        *,
        log_id: int,
        source_path: str,
        request_id: int = 42,
    ) -> None:
        if request_id not in db._requests:  # type: ignore[attr-defined]
            db.seed_request(make_request_row(
                id=request_id,
                mb_release_id="mbid-123",
                status="imported",
                imported_path=source_path,
            ))
        else:
            db.update_request_fields(
                request_id,
                status="imported",
                imported_path=source_path,
            )
        _seed_candidate_for_download_log(
            db, log_id,
            mb_release_id="mbid-candidate-reject",
            files=snapshot_audio_files(source_path),
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=128,
                avg_bitrate_kbps=128,
                median_bitrate_kbps=128,
                format="MP3",
                spectral_grade="genuine",
            ),
            codec="mp3",
            container="mp3",
            storage_format="MP3",
        )
        _seed_current_for_request(
            db, request_id,
            mb_release_id="mbid-current-reject",
            files=snapshot_audio_files(source_path),
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=245,
                avg_bitrate_kbps=245,
                median_bitrate_kbps=245,
                format="MP3",
                spectral_grade="genuine",
            ),
            codec="mp3",
            container="mp3",
            storage_format="MP3",
        )

    def test_force_import_job_calls_existing_dispatch_and_completes(self):
        from scripts import importer

        db = FakePipelineDB()
        job = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            dedupe_key=force_import_dedupe_key(7),
            payload=force_import_payload(
                download_log_id=7,
                failed_path="/tmp/failed",
                source_username="alice",
            ),
        )
        self._mark_importable(db, job)
        claimed = db.claim_next_import_job(worker_id="worker")
        assert claimed is not None

        with patch(
            "lib.import_dispatch.dispatch_import_from_db",
            return_value=DispatchOutcome(True, "imported"),
        ) as dispatch:
            updated = importer.process_claimed_job(cast(Any, db), claimed)

        dispatch.assert_called_once_with(
            db,
            request_id=42,
            failed_path="/tmp/failed",
            force=True,
            outcome_label=IMPORT_JOB_FORCE,
            source_username="alice",
            source_dirs=None,
            import_job_id=claimed.id,
            download_log_id=7,
        )
        assert updated is not None
        self.assertEqual(updated.status, "completed")
        self.assertEqual(self._result(updated)["success"], True)
        self.assertEqual(job.id, updated.id)

    def test_force_import_job_forwards_source_dirs(self):
        from scripts import importer

        db = FakePipelineDB()
        job = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            dedupe_key=force_import_dedupe_key(7),
            payload=force_import_payload(
                download_log_id=7,
                failed_path="/tmp/failed",
                source_username="alice",
                source_dirs=["alice\\Artist\\Album", "alice\\Artist\\Album\\CD2"],
            ),
        )
        self._mark_importable(db, job)
        claimed = db.claim_next_import_job(worker_id="worker")
        assert claimed is not None

        with patch(
            "lib.import_dispatch.dispatch_import_from_db",
            return_value=DispatchOutcome(True, "imported"),
        ) as dispatch:
            importer.process_claimed_job(cast(Any, db), claimed)

        dispatch.assert_called_once_with(
            db,
            request_id=42,
            failed_path="/tmp/failed",
            force=True,
            outcome_label=IMPORT_JOB_FORCE,
            source_username="alice",
            source_dirs=["alice\\Artist\\Album", "alice\\Artist\\Album\\CD2"],
            import_job_id=claimed.id,
            download_log_id=7,
        )

    def test_force_import_job_does_not_forward_preview_import_result(self):
        from scripts import importer

        preview_ir = ImportResult(
            decision="import",
            new_measurement=AudioQualityMeasurement(min_bitrate_kbps=245),
        )
        db = FakePipelineDB()
        job = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            dedupe_key=force_import_dedupe_key(7),
            payload=force_import_payload(
                download_log_id=7,
                failed_path="/tmp/failed",
                source_username="alice",
            ),
        )
        self._mark_importable(
            db,
            job,
            preview_result={
                "verdict": "would_import",
                "import_result": json.loads(preview_ir.to_json()),
            },
        )
        claimed = db.claim_next_import_job(worker_id="worker")
        assert claimed is not None

        with patch(
            "lib.import_dispatch.dispatch_import_from_db",
            return_value=DispatchOutcome(True, "imported"),
        ) as dispatch:
            importer.process_claimed_job(cast(Any, db), claimed)

        self.assertNotIn("preview_import_result", dispatch.call_args.kwargs)

    def test_force_import_job_does_not_forward_stale_preview_import_result_as_authority(self):
        from scripts import importer

        preview_ir = ImportResult(
            decision="import",
            already_in_beets=False,
            new_measurement=AudioQualityMeasurement(min_bitrate_kbps=141),
        )
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            status="manual",
            min_bitrate=116,
            verified_lossless=False,
            current_spectral_grade="likely_transcode",
            current_lossless_source_v0_probe_avg_bitrate=240,
        ))
        job = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            dedupe_key=force_import_dedupe_key(7),
            payload=force_import_payload(
                download_log_id=7,
                failed_path="/tmp/failed",
                source_username="alice",
            ),
        )
        self._mark_importable(
            db,
            job,
            preview_result={
                "verdict": "would_import",
                "import_result": json.loads(preview_ir.to_json()),
            },
        )
        claimed = db.claim_next_import_job(worker_id="worker")
        assert claimed is not None

        with patch(
            "lib.import_dispatch.dispatch_import_from_db",
            return_value=DispatchOutcome(True, "imported"),
        ) as dispatch:
            importer.process_claimed_job(cast(Any, db), claimed)

        self.assertNotIn(
            "preview_import_result",
            dispatch.call_args.kwargs,
            "Stored preview ImportResult is audit/evidence input only; force "
            "import must recompute the action decision against current evidence.",
        )

    def test_manual_import_failure_marks_job_failed(self):
        from scripts import importer

        db = FakePipelineDB()
        job = db.enqueue_import_job(
            IMPORT_JOB_MANUAL,
            request_id=42,
            dedupe_key=manual_import_dedupe_key(42, "/tmp/manual"),
            payload=manual_import_payload(failed_path="/tmp/manual"),
        )
        self._mark_importable(db, job)
        claimed = db.claim_next_import_job(worker_id="worker")
        assert claimed is not None

        with patch(
            "lib.import_dispatch.dispatch_import_from_db",
            return_value=DispatchOutcome(False, "quality gate rejected"),
        ):
            updated = importer.process_claimed_job(cast(Any, db), claimed)

        assert updated is not None
        self.assertEqual(updated.status, "failed")
        self.assertEqual(updated.error, "quality gate rejected")
        self.assertEqual(self._result(updated)["success"], False)

    def test_failed_force_import_quality_pipeline_reject_cleans_without_redeciding(self):
        from scripts import importer

        db = FakePipelineDB()
        root, source = _make_failed_import_source()
        try:
            with open(os.path.join(source, "01.mp3"), "wb") as f:
                f.write(b"audio")
            log_id = self._log_wrong_match(db, failed_path=source)
            job = db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                dedupe_key=force_import_dedupe_key(log_id),
                payload=force_import_payload(
                    download_log_id=log_id,
                    failed_path=source,
                    source_username="alice",
                ),
            )
            self._mark_importable(db, job)
            claimed = db.claim_next_import_job(worker_id="worker")
            assert claimed is not None

            with patch(
                "lib.import_dispatch.dispatch_import_from_db",
                return_value=DispatchOutcome(
                    False,
                    "Rejected by persisted quality evidence: downgrade",
                    code=DISPATCH_CODE_QUALITY_PIPELINE_REJECTED,
                ),
            ), patch(
                "lib.wrong_match_cleanup_service.cleanup_wrong_match",
            ) as cleanup_wrong_match:
                with patch(
                    "lib.wrong_match_cleanup_service.full_pipeline_decision_from_evidence",
                    side_effect=AssertionError("cleanup must not re-decide"),
                ), patch(
                    "lib.quality.full_pipeline_decision_from_evidence",
                    side_effect=AssertionError("cleanup must not re-decide"),
                ):
                    updated = importer.process_claimed_job(cast(Any, db), claimed)

            cleanup_wrong_match.assert_not_called()
            assert updated is not None
            self.assertEqual(updated.status, "failed")
            self.assertFalse(os.path.exists(source))
            self.assertEqual(db.get_wrong_matches(), [])
            result = self._result(updated)
            self.assertEqual(result["cleanup"]["success"], True)
            self.assertEqual(result["cleanup"]["outcome"], "deleted")
            self.assertEqual(result["cleanup"]["cleared_rows"], 1)
            self.assertEqual(result["cleanup"]["reason"], "quality_pipeline_rejected")
            self.assertEqual(
                result["cleanup"]["dispatch_code"],
                DISPATCH_CODE_QUALITY_PIPELINE_REJECTED,
            )
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_failed_force_import_non_pipeline_failure_preserves_wrong_match(self):
        from scripts import importer

        db = FakePipelineDB()
        root, source = _make_failed_import_source()
        try:
            with open(os.path.join(source, "01.mp3"), "wb") as f:
                f.write(b"audio")
            db.seed_request(make_request_row(
                id=42,
                mb_release_id="mbid-123",
                status="manual",
            ))
            log_id = self._log_wrong_match(db, failed_path=source)
            job = db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                dedupe_key=force_import_dedupe_key(log_id),
                payload=force_import_payload(
                    download_log_id=log_id,
                    failed_path=source,
                    source_username="alice",
                ),
            )
            self._mark_importable(
                db,
                job,
                preview_result={
                    "verdict": "confident_reject",
                    "confident_reject": True,
                    "cleanup_eligible": True,
                },
            )
            claimed = db.claim_next_import_job(worker_id="worker")
            assert claimed is not None

            with patch(
                "lib.import_dispatch.dispatch_import_from_db",
                return_value=DispatchOutcome(False, "beets failed"),
            ), patch(
                "lib.wrong_match_cleanup_service.cleanup_wrong_match",
            ) as cleanup_wrong_match:
                updated = importer.process_claimed_job(cast(Any, db), claimed)

            cleanup_wrong_match.assert_not_called()
            assert updated is not None
            self.assertEqual(updated.status, "failed")
            self.assertTrue(os.path.isdir(source))
            self.assertEqual(len(db.get_wrong_matches()), 1)
            cleanup = self._result(updated)["cleanup"]
            self.assertTrue(cleanup["skipped"])
            self.assertEqual(
                cleanup["outcome"],
                "skipped_non_quality_pipeline_failure",
            )
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_force_import_extra_audio_keeps_wm_and_self_heals_end_to_end(self):
        """Issue #387 composition: force-importing a folder with extra audio,
        through the REAL manifest guard (no mocked dispatch).

        Proves the two halves compose: the guard self-heals the request to
        ``wanted`` (R20) AND its audit row does NOT inflate Wrong Matches,
        while the importer preserves the original WM entry for review (the
        ``IMPORT_MANIFEST_REJECTED`` code skips cleanup). beets never runs —
        the guard rejects upstream of it.
        """
        from scripts import importer

        db = FakePipelineDB()
        root, source = _make_failed_import_source()
        try:
            with open(os.path.join(source, "01.mp3"), "wb") as f:
                f.write(b"audio")
            with open(os.path.join(source, "bonus.mp3"), "wb") as f:
                f.write(b"audio")
            db.seed_request(make_request_row(
                id=42,
                mb_release_id="mbid-123",
                status="manual",
            ))
            # One expected track but two audio files on disk → extra audio.
            db.set_tracks(42, [{"track_number": 1, "title": "One"}])
            log_id = self._log_wrong_match(db, failed_path=source)
            job = db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                dedupe_key=force_import_dedupe_key(log_id),
                payload=force_import_payload(
                    download_log_id=log_id,
                    failed_path=source,
                    source_username="alice",
                ),
            )
            self._mark_importable(db, job)
            claimed = db.claim_next_import_job(worker_id="worker")
            assert claimed is not None

            # No dispatch patch — the real guard runs. beets is never reached.
            updated = importer.process_claimed_job(cast(Any, db), claimed)

            assert updated is not None
            self.assertEqual(updated.status, "failed")
            # R20: the album is still wanted; the request self-heals.
            self.assertEqual(db.request(42)["status"], "wanted")
            # The dead WM entry is preserved (extra audio → operator review),
            # and the audit row did NOT create a second entry.
            self.assertEqual(len(db.get_wrong_matches()), 1)
            self.assertTrue(os.path.isdir(source))
            outcomes = [
                (log.outcome, log.beets_scenario) for log in db.download_logs
            ]
            self.assertIn(("rejected", "untracked_audio"), outcomes)
            cleanup = self._result(updated)["cleanup"]
            self.assertTrue(cleanup["skipped"])
            self.assertEqual(len(db.denylist), 0)
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_force_import_undercount_preserves_folder_and_self_heals_end_to_end(self):
        """Issue #387 regression: force-importing an UNDER-COUNT folder (fewer
        audio files than the request's track rows, no extras) must NOT delete
        the operator's partial audio.

        An under-count source physically contains audio the operator chose to
        import — it is not 'nothing to inspect'. The guard self-heals the
        request to ``wanted`` (R20) but returns ``IMPORT_MANIFEST_REJECTED``
        so the importer PRESERVES the folder (``_cleanup_failed_force_import``
        skips deletion on that code). Routing it through
        ``QUALITY_PIPELINE_REJECTED`` would ``shutil.rmtree`` the only
        surviving copy — the exact irreversible auto-decision the archivist
        frame forbids. Wrong-match deletion is reserved for the genuinely
        empty (0-file) case, which routes through the evidence pipeline, not
        this guard.
        """
        from scripts import importer

        db = FakePipelineDB()
        root, source = _make_failed_import_source()
        try:
            # One disc on disk; the request expects two tracks → under-count.
            with open(os.path.join(source, "01.mp3"), "wb") as f:
                f.write(b"audio")
            db.seed_request(make_request_row(
                id=42,
                mb_release_id="mbid-123",
                status="manual",
            ))
            db.set_tracks(42, [
                {"track_number": 1, "title": "One"},
                {"track_number": 2, "title": "Two"},
            ])
            log_id = self._log_wrong_match(db, failed_path=source)
            job = db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                dedupe_key=force_import_dedupe_key(log_id),
                payload=force_import_payload(
                    download_log_id=log_id,
                    failed_path=source,
                    source_username="alice",
                ),
            )
            self._mark_importable(db, job)
            claimed = db.claim_next_import_job(worker_id="worker")
            assert claimed is not None

            updated = importer.process_claimed_job(cast(Any, db), claimed)

            assert updated is not None
            self.assertEqual(updated.status, "failed")
            # THE regression assertion: the operator's partial audio survives.
            self.assertTrue(
                os.path.isdir(source),
                "under-count force-import must NOT delete the operator's folder")
            self.assertTrue(os.path.isfile(os.path.join(source, "01.mp3")))
            # R20: the album is still wanted; the request self-heals.
            self.assertEqual(db.request(42)["status"], "wanted")
            # WM entry preserved (something to inspect), no duplicate.
            self.assertEqual(len(db.get_wrong_matches()), 1)
            outcomes = [
                (log.outcome, log.beets_scenario) for log in db.download_logs
            ]
            self.assertIn(("rejected", "incomplete_fileset"), outcomes)
            cleanup = self._result(updated)["cleanup"]
            self.assertTrue(cleanup["skipped"])
            self.assertEqual(len(db.denylist), 0)
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_force_import_requeued_for_preview_does_not_mark_failed(self):
        """U2: when dispatch returns DISPATCH_CODE_REQUEUED_FOR_PREVIEW the
        importer does NOT write a terminal failed status and does NOT run
        the wrong-match cleanup path. The dispatch-side requeue has already
        flipped the row back to queued/waiting; the importer just logs and
        yields."""
        from scripts import importer
        from lib.import_dispatch import DISPATCH_CODE_REQUEUED_FOR_PREVIEW

        db = FakePipelineDB()
        root, source = _make_failed_import_source()
        try:
            with open(os.path.join(source, "01.mp3"), "wb") as f:
                f.write(b"audio")
            log_id = self._log_wrong_match(db, failed_path=source)
            job = db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                dedupe_key=force_import_dedupe_key(log_id),
                payload=force_import_payload(
                    download_log_id=log_id,
                    failed_path=source,
                    source_username="alice",
                ),
            )
            self._mark_importable(db, job)
            claimed = db.claim_next_import_job(worker_id="worker")
            assert claimed is not None
            claimed_attempts = claimed.attempts

            def fake_dispatch(*_args, **_kwargs):
                # Simulate the dispatch-side requeue.
                db.requeue_import_job_for_preview(
                    job.id,
                    reason="candidate evidence missing",
                )
                return DispatchOutcome(
                    False,
                    "Candidate evidence unavailable; requeued for preview",
                    code=DISPATCH_CODE_REQUEUED_FOR_PREVIEW,
                )

            with patch(
                "lib.import_dispatch.dispatch_import_from_db",
                side_effect=fake_dispatch,
            ), patch(
                "lib.wrong_match_cleanup_service.cleanup_wrong_match",
            ) as cleanup:
                updated = importer.process_claimed_job(cast(Any, db), claimed)

            # Importer must NOT have written a terminal status.
            cleanup.assert_not_called()
            self.assertTrue(os.path.isdir(source))
            # Job row is queued/waiting after the requeue.
            row = next(r for r in db._import_jobs if r["id"] == job.id)
            self.assertEqual(row["status"], "queued")
            self.assertEqual(row["preview_status"], "waiting")
            # Importer did not retry-count: row attempts not bumped beyond
            # the original claim.
            self.assertEqual(row["attempts"], claimed_attempts)
            # process_claimed_job returns the job ImportJob (current state),
            # not a terminal failure. The job should not be in 'failed'.
            if updated is not None:
                self.assertNotEqual(updated.status, "failed")
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_force_import_requeue_failed_marks_job_failed(self):
        """REL-001: when dispatch returns DISPATCH_CODE_REQUEUE_FAILED (its
        requeue UPDATE itself raised), the importer must mark the job
        terminally failed rather than leaving it running. Leaving the job
        running would let `requeue_running_import_jobs` on next worker boot
        reclaim it — but the importer's claim query still matches
        preview_status='evidence_ready', so it would re-claim, hit the same
        requeue condition, fail again, and spin forever. Failing terminally
        surfaces the issue to ops; the operator re-triggers once the DB
        problem is resolved.
        """
        from scripts import importer
        from lib.import_dispatch import DISPATCH_CODE_REQUEUE_FAILED

        db = FakePipelineDB()
        root, source = _make_failed_import_source()
        try:
            with open(os.path.join(source, "01.mp3"), "wb") as f:
                f.write(b"audio")
            log_id = self._log_wrong_match(db, failed_path=source)
            job = db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                dedupe_key=force_import_dedupe_key(log_id),
                payload=force_import_payload(
                    download_log_id=log_id,
                    failed_path=source,
                    source_username="alice",
                ),
            )
            self._mark_importable(db, job)
            claimed = db.claim_next_import_job(worker_id="worker")
            assert claimed is not None

            with patch(
                "lib.import_dispatch.dispatch_import_from_db",
                return_value=DispatchOutcome(
                    False,
                    "requeue UPDATE failed: boom",
                    code=DISPATCH_CODE_REQUEUE_FAILED,
                ),
            ), patch(
                "lib.wrong_match_cleanup_service.cleanup_wrong_match",
            ) as cleanup:
                updated = importer.process_claimed_job(cast(Any, db), claimed)

            # No wrong-match cleanup runs on the requeue-failed path (the
            # situation is a DB issue, not a quality decision).
            cleanup.assert_not_called()
            row = next(r for r in db._import_jobs if r["id"] == job.id)
            self.assertEqual(row["status"], "failed")
            self.assertIn("requeue", row["message"])
            self.assertTrue(os.path.isdir(source))
            assert updated is not None
            self.assertEqual(updated.status, "failed")
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_failed_force_import_job_clears_newer_duplicate_rejection(self):
        from scripts import importer

        db = FakePipelineDB()
        root, source = _make_failed_import_source()
        try:
            with open(os.path.join(source, "01.mp3"), "wb") as f:
                f.write(b"audio")
            log_id = self._log_wrong_match(db, failed_path=source, username="old")
            job = db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                dedupe_key=force_import_dedupe_key(log_id),
                payload=force_import_payload(
                    download_log_id=log_id,
                    failed_path=source,
                    source_username="alice",
                ),
            )
            self._mark_importable(db, job)
            claimed = db.claim_next_import_job(worker_id="worker")
            assert claimed is not None

            def reject_again(*_args, **kwargs):
                db.log_download(
                    kwargs["request_id"],
                    soulseek_username="new",
                    outcome="rejected",
                    validation_result={
                        "scenario": "quality_downgrade",
                        "failed_path": kwargs["failed_path"],
                    },
                )
                return DispatchOutcome(
                    False,
                    "Rejected by persisted quality evidence: downgrade",
                    code=DISPATCH_CODE_QUALITY_PIPELINE_REJECTED,
                )

            with patch(
                "lib.import_dispatch.dispatch_import_from_db",
                side_effect=reject_again,
            ):
                updated = importer.process_claimed_job(cast(Any, db), claimed)

            assert updated is not None
            self.assertEqual(updated.status, "failed")
            self.assertEqual(self._result(updated)["cleanup"]["cleared_rows"], 2)
            self.assertEqual(db.get_wrong_matches(), [])
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_failed_force_import_quality_reject_skips_cleanup_for_other_active_job(self):
        from scripts import importer

        db = FakePipelineDB()
        root, source = _make_failed_import_source()
        try:
            with open(os.path.join(source, "01.mp3"), "wb") as f:
                f.write(b"audio")
            log_id = self._log_wrong_match(db, failed_path=source)
            job = db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                dedupe_key=force_import_dedupe_key(log_id),
                payload=force_import_payload(
                    download_log_id=log_id,
                    failed_path=source,
                    source_username="alice",
                ),
            )
            self._mark_importable(db, job)
            claimed = db.claim_next_import_job(worker_id="worker")
            assert claimed is not None
            db.enqueue_import_job(
                IMPORT_JOB_MANUAL,
                request_id=42,
                dedupe_key=manual_import_dedupe_key(42, source),
                payload=manual_import_payload(failed_path=source),
            )

            with patch(
                "lib.import_dispatch.dispatch_import_from_db",
                return_value=DispatchOutcome(
                    False,
                    "Rejected by persisted quality evidence: downgrade",
                    code=DISPATCH_CODE_QUALITY_PIPELINE_REJECTED,
                ),
            ):
                updated = importer.process_claimed_job(cast(Any, db), claimed)

            assert updated is not None
            self.assertEqual(updated.status, "failed")
            self.assertTrue(os.path.isdir(source))
            self.assertEqual(len(db.get_wrong_matches()), 1)
            cleanup = self._result(updated)["cleanup"]
            self.assertTrue(cleanup["skipped"])
            self.assertEqual(cleanup["outcome"], "skipped_active_job")
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_manual_import_failure_preserves_source_and_wrong_match(self):
        from scripts import importer

        db = FakePipelineDB()
        root, source = _make_failed_import_source()
        try:
            self._log_wrong_match(db, failed_path=source)
            job = db.enqueue_import_job(
                IMPORT_JOB_MANUAL,
                request_id=42,
                dedupe_key=manual_import_dedupe_key(42, source),
                payload=manual_import_payload(failed_path=source),
            )
            self._mark_importable(db, job)
            claimed = db.claim_next_import_job(worker_id="worker")
            assert claimed is not None

            with patch(
                "lib.import_dispatch.dispatch_import_from_db",
                return_value=DispatchOutcome(False, "manual import failed"),
            ):
                updated = importer.process_claimed_job(cast(Any, db), claimed)

            assert updated is not None
            self.assertEqual(updated.status, "failed")
            self.assertTrue(os.path.isdir(source))
            self.assertEqual(len(db.get_wrong_matches()), 1)
            self.assertNotIn("cleanup", self._result(updated))
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_deferred_force_import_preserves_source_and_wrong_match(self):
        from scripts import importer

        db = FakePipelineDB()
        root, source = _make_failed_import_source()
        try:
            log_id = self._log_wrong_match(db, failed_path=source)
            job = db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                dedupe_key=force_import_dedupe_key(log_id),
                payload=force_import_payload(
                    download_log_id=log_id,
                    failed_path=source,
                ),
            )
            self._mark_importable(db, job)
            claimed = db.claim_next_import_job(worker_id="worker")
            assert claimed is not None

            with patch(
                "lib.import_dispatch.dispatch_import_from_db",
                return_value=DispatchOutcome(
                    False,
                    "Another import is already in progress",
                    deferred=True,
                ),
            ):
                updated = importer.process_claimed_job(cast(Any, db), claimed)

            assert updated is not None
            self.assertEqual(updated.status, "failed")
            self.assertTrue(os.path.isdir(source))
            self.assertEqual(len(db.get_wrong_matches()), 1)
            self.assertNotIn("cleanup", self._result(updated))
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_startup_requeues_abandoned_running_job_for_retry(self):
        from scripts import importer

        db = FakePipelineDB()
        job = db.enqueue_import_job(
            IMPORT_JOB_MANUAL,
            request_id=42,
            dedupe_key=manual_import_dedupe_key(42, "/tmp/manual"),
            payload=manual_import_payload(failed_path="/tmp/manual"),
        )
        self._mark_importable(db, job)
        claimed = db.claim_next_import_job(worker_id="old-worker")
        assert claimed is not None

        recovered = importer.recover_abandoned_running_jobs(cast(Any, db))

        self.assertEqual([job.id for job in recovered], [claimed.id])
        self.assertEqual(recovered[0].status, "queued")
        self.assertIsNone(recovered[0].worker_id)
        self.assertIsNone(recovered[0].heartbeat_at)

        with patch(
            "lib.import_dispatch.dispatch_import_from_db",
            return_value=DispatchOutcome(True, "imported on retry"),
        ):
            updated = importer.run_once(cast(Any, db), worker_id="new-worker")

        assert updated is not None
        self.assertEqual(updated.status, "completed")
        retried = db.get_import_job(claimed.id)
        assert retried is not None
        self.assertEqual(retried.attempts, 2)

    def test_importer_does_not_claim_job_waiting_for_preview(self):
        from scripts import importer

        db = FakePipelineDB()
        db.enqueue_import_job(
            IMPORT_JOB_MANUAL,
            request_id=42,
            dedupe_key=manual_import_dedupe_key(42, "/tmp/manual"),
            payload=manual_import_payload(failed_path="/tmp/manual"),
        )

        self.assertIsNone(importer.run_once(cast(Any, db), worker_id="worker"))

    def test_automation_job_reconstructs_active_state_and_uses_processing_path(self):
        from scripts import importer

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            status="downloading",
            active_download_state={
                "filetype": "flac",
                "enqueued_at": "2026-04-25T00:00:00+00:00",
                "files": [{
                    "username": "alice",
                    "filename": "Artist\\Album\\01.flac",
                    "file_dir": "Artist\\Album",
                    "size": 123,
                }],
            },
        ))
        job = db.enqueue_import_job(
            IMPORT_JOB_AUTOMATION,
            request_id=42,
            dedupe_key=automation_import_dedupe_key(42),
            payload={},
        )
        self._mark_importable(db, job)
        claimed = db.claim_next_import_job(worker_id="worker")
        assert claimed is not None

        with patch(
            "lib.download._run_completed_processing",
            return_value=True,
        ) as processing:
            updated = importer.process_claimed_job(
                cast(Any, db),
                claimed,
                ctx=object(),
            )

        processing.assert_called_once()
        assert updated is not None
        self.assertEqual(updated.status, "completed")
        self.assertEqual(updated.message, "Automation import processing completed")

    def test_automation_job_completes_from_dispatch_outcome(self):
        from scripts import importer

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            status="downloading",
            active_download_state={
                "filetype": "flac",
                "enqueued_at": "2026-04-25T00:00:00+00:00",
                "files": [{
                    "username": "alice",
                    "filename": "Artist\\Album\\01.flac",
                    "file_dir": "Artist\\Album",
                    "size": 123,
                }],
            },
        ))
        job = db.enqueue_import_job(
            IMPORT_JOB_AUTOMATION,
            request_id=42,
            dedupe_key=automation_import_dedupe_key(42),
            payload={},
        )
        self._mark_importable(db, job)
        claimed = db.claim_next_import_job(worker_id="worker")
        assert claimed is not None

        with patch(
            "lib.download._run_completed_processing",
            return_value=DispatchOutcome(True, "Imported by dispatch"),
        ) as processing:
            updated = importer.process_claimed_job(
                cast(Any, db),
                claimed,
                ctx=object(),
            )

        self.assertEqual(processing.call_args.kwargs["import_job_id"], job.id)
        assert updated is not None
        self.assertEqual(updated.status, "completed")
        self.assertEqual(updated.message, "Imported by dispatch")
        self.assertEqual(self._result(updated)["success"], True)

    def test_automation_job_fails_from_dispatch_outcome(self):
        from scripts import importer

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            status="downloading",
            active_download_state={
                "filetype": "flac",
                "enqueued_at": "2026-04-25T00:00:00+00:00",
                "files": [{
                    "username": "alice",
                    "filename": "Artist\\Album\\01.flac",
                    "file_dir": "Artist\\Album",
                    "size": 123,
                }],
            },
        ))
        job = db.enqueue_import_job(
            IMPORT_JOB_AUTOMATION,
            request_id=42,
            dedupe_key=automation_import_dedupe_key(42),
            payload={},
        )
        self._mark_importable(db, job)
        claimed = db.claim_next_import_job(worker_id="worker")
        assert claimed is not None

        with patch(
            "lib.download._run_completed_processing",
            return_value=DispatchOutcome(False, "Pre-import gate rejected"),
        ):
            updated = importer.process_claimed_job(
                cast(Any, db),
                claimed,
                ctx=object(),
            )

        assert updated is not None
        self.assertEqual(updated.status, "failed")
        self.assertEqual(updated.message, "Pre-import gate rejected")
        self.assertEqual(updated.error, "Pre-import gate rejected")
        self.assertEqual(self._result(updated)["success"], False)

    def test_requeued_automation_job_abandons_interrupted_auto_import(self):
        from scripts import importer
        from lib.processing_paths import stage_to_ai_path

        with tempfile.TemporaryDirectory() as tmpdir:
            staging_root = os.path.join(tmpdir, "staging")
            slskd_root = os.path.join(tmpdir, "slskd")
            os.makedirs(staging_root)
            os.makedirs(slskd_root)
            staged_path = stage_to_ai_path(
                artist="Test Artist",
                title="Test Album",
                staging_dir=staging_root,
                request_id=42,
                auto_import=True,
            )
            os.makedirs(staged_path)
            with open(os.path.join(staged_path, "01.opus"), "w") as fp:
                fp.write("converted audio")

            db = FakePipelineDB()
            db.seed_request(make_request_row(
                id=42,
                status="downloading",
                artist_name="Test Artist",
                album_title="Test Album",
                year=2020,
                mb_release_id="test-mbid",
                active_download_state={
                    "filetype": "flac",
                    "enqueued_at": "2026-04-25T00:00:00+00:00",
                    "processing_started_at": "2026-04-25T00:10:00+00:00",
                    "import_subprocess_started_at": "2026-04-25T00:11:00+00:00",
                    "current_path": staged_path,
                    "files": [{
                        "username": "alice",
                        "filename": "Artist\\Album\\01.flac",
                        "file_dir": "Artist\\Album",
                        "size": 123,
                    }],
                },
            ))
            job = db.enqueue_import_job(
                IMPORT_JOB_AUTOMATION,
                request_id=42,
                dedupe_key=automation_import_dedupe_key(42),
                payload={},
            )
            self._mark_importable(db, job)
            claimed = db.claim_next_import_job(worker_id="old-worker")
            assert claimed is not None

            recovered = importer.recover_abandoned_running_jobs(cast(Any, db))
            self.assertEqual([job.id for job in recovered], [claimed.id])

            cfg = type("Cfg", (), {
                "slskd_download_dir": slskd_root,
                "beets_staging_dir": staging_root,
                "beets_validation_enabled": False,
            })()
            ctx = make_ctx_with_fake_db(db, cfg=cfg)
            updated = importer.run_once(cast(Any, db), worker_id="new-worker", ctx=ctx)

            assert updated is not None
            self.assertEqual(updated.status, "failed")
            self.assertEqual(db.request(42)["status"], "wanted")
            self.assertEqual(db.get_active_import_job_for_request(42), None)
            self.assertFalse(os.path.exists(staged_path))
            failed_parent = os.path.join(os.path.dirname(staged_path), "failed_imports")
            moved = os.listdir(failed_parent)
            self.assertEqual(len(moved), 1)
            self.assertTrue(moved[0].startswith("abandoned_auto_import"))
            self.assertTrue(os.path.exists(os.path.join(
                failed_parent,
                moved[0],
                "01.opus",
            )))
            self.assertEqual(len(db.download_logs), 1)
            db.assert_log(
                self,
                0,
                outcome="failed",
                beets_scenario="abandoned_auto_import",
            )
            self.assertEqual(db.denylist, [])
            self.assertEqual(db.cooldowns_applied, [])


class TestImportPreviewWorker(unittest.TestCase):
    def _preview(
        self,
        verdict: str,
        *,
        reason: str | None = None,
        source_path: str | None = None,
    ) -> ImportPreviewResult:
        """Build a preview result for worker tests.

        After U5 the worker emits only ``evidence_ready`` and
        ``measurement_failed``. For backward-compat with existing tests we
        translate the legacy verdict labels:

          * ``would_import`` / ``confident_reject`` → ``evidence_ready`` (the
            importer would have read these and decided; in U5 onward, the
            importer reads the persisted evidence instead).
          * ``uncertain`` → ``measurement_failed`` (preview could not produce
            evidence; self-healing finalize fires).

        Explicit ``evidence_ready`` / ``measurement_failed`` callers get those
        verdicts unchanged.
        """
        from lib.quality import MeasurementFailure

        if verdict in ("would_import", "evidence_ready"):
            translated = "evidence_ready"
            failure = None
        elif verdict in ("uncertain", "confident_reject", "measurement_failed"):
            translated = "measurement_failed"
            failure = MeasurementFailure(
                reason="measurement_crashed",
                detail=reason or verdict,
                source_path=source_path or "",
            )
        else:
            translated = verdict
            failure = None
        return ImportPreviewResult(
            mode="path",
            verdict=translated,
            would_import=verdict == "would_import",
            confident_reject=verdict == "confident_reject",
            uncertain=verdict == "uncertain",
            decision=reason,
            reason=reason,
            stage_chain=[f"preview:{reason or verdict}"],
            source_path=source_path,
            failure=failure,
        )

    def _seed_job_candidate_evidence(
        self,
        db: FakePipelineDB,
        job_id: int,
        source_path: str,
    ) -> None:
        _seed_candidate_for_import_job(
            db, job_id,
            mb_release_id=f"mbid-job-{job_id}",
            files=snapshot_audio_files(source_path),
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=245,
                avg_bitrate_kbps=256,
                median_bitrate_kbps=252,
                format="MP3 V0",
                spectral_grade="genuine",
            ),
            codec="mp3",
            container="mp3",
            storage_format="mp3 v0",
        )

    def test_force_job_preview_would_import_marks_importable(self):
        from scripts import import_preview_worker

        with tempfile.TemporaryDirectory() as source:
            with open(os.path.join(source, "01.mp3"), "wb") as handle:
                handle.write(b"audio")
            db = FakePipelineDB()
            db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                dedupe_key=force_import_dedupe_key(7),
                payload=force_import_payload(
                    download_log_id=7,
                    failed_path=source,
                    source_username="alice",
                ),
            )
            claimed = db.claim_next_import_preview_job(worker_id="preview")
            assert claimed is not None

            preview_result = self._preview(
                "would_import",
                reason="import",
                source_path=source,
            )

            def fake_preview(*args: Any, **kwargs: Any) -> ImportPreviewResult:
                # Simulate production: preview measures + persists evidence.
                self._seed_job_candidate_evidence(db, claimed.id, source)
                return preview_result

            with patch(
                "scripts.import_preview_worker.measure_and_persist_candidate_evidence",
                side_effect=fake_preview,
            ) as preview:
                updated = import_preview_worker.process_claimed_preview_job(
                    db,
                    claimed,
                )

        preview.assert_called_once_with(
            db,
            request_id=42,
            path=source,
            force=True,
            download_log_id=7,
            import_job_id=claimed.id,
        )
        assert updated is not None
        self.assertEqual(updated.status, "queued")
        self.assertEqual(updated.preview_status, "evidence_ready")
        assert updated.preview_result is not None
        self.assertEqual(updated.preview_result["verdict"], "evidence_ready")
        self.assertIsNotNone(updated.importable_at)

    def test_manual_job_preview_uses_non_force_semantics(self):
        from scripts import import_preview_worker

        with tempfile.TemporaryDirectory() as source:
            with open(os.path.join(source, "01.mp3"), "wb") as handle:
                handle.write(b"audio")
            db = FakePipelineDB()
            db.enqueue_import_job(
                IMPORT_JOB_MANUAL,
                request_id=42,
                dedupe_key=manual_import_dedupe_key(42, source),
                payload=manual_import_payload(failed_path=source),
            )
            claimed = db.claim_next_import_preview_job(worker_id="preview")
            assert claimed is not None

            preview_result = self._preview(
                "would_import",
                reason="import",
                source_path=source,
            )

            def fake_preview(*args: Any, **kwargs: Any) -> ImportPreviewResult:
                self._seed_job_candidate_evidence(db, claimed.id, source)
                return preview_result

            with patch(
                "scripts.import_preview_worker.measure_and_persist_candidate_evidence",
                side_effect=fake_preview,
            ) as preview:
                updated = import_preview_worker.process_claimed_preview_job(
                    db,
                    claimed,
                )

        preview.assert_called_once_with(
            db,
            request_id=42,
            path=source,
            force=False,
            download_log_id=None,
            import_job_id=claimed.id,
        )
        assert updated is not None
        self.assertEqual(updated.preview_status, "evidence_ready")

    def test_automation_job_preview_uses_active_download_current_path(self):
        from scripts import import_preview_worker

        with tempfile.TemporaryDirectory() as staged:
            with open(os.path.join(staged, "01.flac"), "wb") as handle:
                handle.write(b"audio")
            db = FakePipelineDB()
            db.seed_request(make_request_row(
                id=42,
                status="downloading",
                active_download_state={
                    "filetype": "flac",
                    "enqueued_at": "2026-04-25T00:00:00+00:00",
                    "current_path": staged,
                    "files": [{
                        "username": "alice",
                        "filename": "Artist\\Album\\01.flac",
                        "file_dir": "Artist\\Album",
                        "size": 123,
                    }],
                },
            ))
            db.enqueue_import_job(
                IMPORT_JOB_AUTOMATION,
                request_id=42,
                dedupe_key=automation_import_dedupe_key(42),
                payload={},
            )
            claimed = db.claim_next_import_preview_job(worker_id="preview")
            assert claimed is not None

            preview_result = self._preview(
                "would_import",
                reason="import",
                source_path=staged,
            )

            def fake_preview(*args: Any, **kwargs: Any) -> ImportPreviewResult:
                self._seed_job_candidate_evidence(db, claimed.id, staged)
                return preview_result

            with patch(
                "scripts.import_preview_worker.measure_and_persist_candidate_evidence",
                side_effect=fake_preview,
            ) as preview:
                updated = import_preview_worker.process_claimed_preview_job(db, claimed)

            preview.assert_called_once_with(
                db,
                request_id=42,
                path=staged,
                force=False,
                download_log_id=None,
                import_job_id=claimed.id,
            )
            assert updated is not None
            self.assertEqual(updated.preview_status, "evidence_ready")

    def test_automation_preview_reject_with_evidence_marks_ready_for_dispatch(self):
        from scripts import import_preview_worker

        with tempfile.TemporaryDirectory() as staged:
            with open(os.path.join(staged, "01.mp3"), "wb") as handle:
                handle.write(b"audio")
            db = FakePipelineDB()
            db.seed_request(make_request_row(
                id=42,
                status="downloading",
                active_download_state={
                    "filetype": "flac",
                    "enqueued_at": "2026-04-25T00:00:00+00:00",
                    "current_path": staged,
                    "files": [{
                        "username": "alice",
                        "filename": "Artist\\Album\\01.flac",
                        "file_dir": "Artist\\Album",
                        "size": 123,
                    }],
                },
            ))
            db.enqueue_import_job(
                IMPORT_JOB_AUTOMATION,
                request_id=42,
                dedupe_key=automation_import_dedupe_key(42),
                payload={},
            )
            claimed = db.claim_next_import_preview_job(worker_id="preview")
            assert claimed is not None
            self._seed_job_candidate_evidence(db, claimed.id, staged)

            with patch(
                "scripts.import_preview_worker.measure_and_persist_candidate_evidence",
                return_value=self._preview(
                    "confident_reject",
                    reason="spectral_reject",
                    source_path=staged,
                ),
            ):
                updated = import_preview_worker.process_claimed_preview_job(
                    db,
                    claimed,
                )

            assert updated is not None
            self.assertEqual(updated.status, "queued")
            self.assertEqual(updated.preview_status, "evidence_ready")
            self.assertIsNone(updated.preview_error)
            self.assertIsNotNone(db.get_active_import_job_for_request(42))
            claimed_for_import = db.claim_next_import_job(worker_id="importer")
            assert claimed_for_import is not None
            self.assertEqual(claimed_for_import.id, updated.id)

    def test_run_once_heartbeats_while_preview_is_running(self):
        from scripts import import_preview_worker

        with tempfile.TemporaryDirectory() as source:
            with open(os.path.join(source, "01.mp3"), "wb") as handle:
                handle.write(b"audio")
            db = FakePipelineDB()
            setattr(db, "dsn", "postgresql://fake")
            db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                dedupe_key=force_import_dedupe_key(7),
                payload=force_import_payload(
                    download_log_id=7,
                    failed_path=source,
                    source_username="alice",
                ),
            )
            initial_claim = db.claim_next_import_preview_job(worker_id="peek")
            assert initial_claim is not None
            assert initial_claim.preview_heartbeat_at is not None
            db.requeue_stale_import_preview_jobs(
                older_than=timedelta(seconds=-1),
                message="test reset",
            )
            heartbeat_seen = threading.Event()

            def preview(*args: Any, **kwargs: Any) -> ImportPreviewResult:
                baseline = db.get_import_job(initial_claim.id)
                assert baseline is not None
                baseline_heartbeat = baseline.preview_heartbeat_at
                assert baseline_heartbeat is not None
                deadline = time.monotonic() + 0.5
                while time.monotonic() < deadline:
                    current = db.get_import_job(initial_claim.id)
                    assert current is not None
                    if (
                        current.preview_heartbeat_at is not None
                        and current.preview_heartbeat_at > baseline_heartbeat
                    ):
                        heartbeat_seen.set()
                        break
                    time.sleep(0.005)
                # Simulate production: preview persists evidence as a
                # side-effect so the post-measurement gate sees it.
                self._seed_job_candidate_evidence(db, initial_claim.id, source)
                return self._preview(
                    "would_import",
                    reason="import",
                    source_path=source,
                )

            with (
                patch("scripts.import_preview_worker.PipelineDB",
                      side_effect=lambda dsn: db),
                patch(
                    "scripts.import_preview_worker.measure_and_persist_candidate_evidence",
                    side_effect=preview,
                ),
            ):
                updated = import_preview_worker.run_once(
                    cast(Any, db),
                    worker_id="preview",
                    heartbeat_interval=0.01,
                )

            assert updated is not None
            self.assertEqual(updated.preview_status, "evidence_ready")
            self.assertTrue(heartbeat_seen.is_set())

    def test_preview_recovery_loop_requeues_abandoned_running_rows(self):
        from scripts import import_preview_worker

        db = FakePipelineDB()
        dsn = "postgresql://fake"
        setattr(db, "dsn", dsn)
        db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            dedupe_key=force_import_dedupe_key(7),
            payload=force_import_payload(
                download_log_id=7,
                failed_path="/tmp/failed",
            ),
        )
        claimed = db.claim_next_import_preview_job(worker_id="dead-worker")
        assert claimed is not None
        old = datetime.now(timezone.utc) - timedelta(hours=2)
        for row in db._import_jobs:
            if row["id"] == claimed.id:
                row["preview_started_at"] = old
                row["preview_heartbeat_at"] = old
                row["updated_at"] = old

        stop = threading.Event()
        thread = threading.Thread(
            target=import_preview_worker.preview_recovery_loop,
            kwargs={
                "dsn": dsn,
                "stop": stop,
                "interval": 0.01,
                "db_factory": lambda dsn: db,
            },
        )
        thread.start()
        try:
            deadline = time.monotonic() + 0.5
            recovered = None
            while time.monotonic() < deadline:
                recovered = db.get_import_job(claimed.id)
                if recovered is not None and recovered.preview_status == "waiting":
                    break
                time.sleep(0.005)
        finally:
            stop.set()
            thread.join(timeout=1.0)

        assert recovered is not None
        self.assertEqual(recovered.preview_status, "waiting")
        self.assertEqual(
            recovered.preview_message,
            import_preview_worker.STALE_PREVIEW_MESSAGE,
        )

    def test_confident_reject_fails_job_without_denylisting_source(self):
        """Post-U5: legacy ``confident_reject`` translates to ``measurement_failed``.

        The ``_preview`` helper translates ``confident_reject`` → ``measurement_failed``;
        the worker routes it through U4's self-healing helper, marking the job
        ``status='failed'`` with ``preview_status='measurement_failed'``. No
        denylist write fires (preview measurement failures are infrastructure-
        class, not user-induced).
        """
        from scripts import import_preview_worker

        db = FakePipelineDB()
        db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            dedupe_key=force_import_dedupe_key(7),
            payload=force_import_payload(
                download_log_id=7,
                failed_path="/tmp/failed",
                source_username="alice",
            ),
        )
        claimed = db.claim_next_import_preview_job(worker_id="preview")
        assert claimed is not None

        with patch(
            "scripts.import_preview_worker.measure_and_persist_candidate_evidence",
            return_value=self._preview("confident_reject", reason="spectral_reject"),
        ):
            updated = import_preview_worker.process_claimed_preview_job(db, claimed)

        assert updated is not None
        self.assertEqual(updated.status, "failed")
        self.assertEqual(updated.preview_status, "measurement_failed")
        self.assertEqual(db.get_denylisted_users(42), [])

    def test_uncertain_preview_fails_without_denylisting(self):
        """Post-U5: legacy ``uncertain`` translates to ``measurement_failed``.

        U4's self-healing helper writes a ``download_log`` row with
        ``outcome='measurement_failed'`` and finalizes the parent request to
        ``wanted`` so the poll loop's active-import-job guard releases.
        """
        from scripts import import_preview_worker

        db = FakePipelineDB()
        db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            dedupe_key=force_import_dedupe_key(7),
            payload=force_import_payload(
                download_log_id=7,
                failed_path="/tmp/failed",
                source_username="alice",
            ),
        )
        claimed = db.claim_next_import_preview_job(worker_id="preview")
        assert claimed is not None

        with patch(
            "scripts.import_preview_worker.measure_and_persist_candidate_evidence",
            return_value=self._preview("uncertain", reason="path_missing"),
        ):
            updated = import_preview_worker.process_claimed_preview_job(db, claimed)

        assert updated is not None
        self.assertEqual(updated.status, "failed")
        self.assertEqual(updated.preview_status, "measurement_failed")
        self.assertEqual(db.get_denylisted_users(42), [])

    def test_threaded_worker_exits_nonzero_when_worker_thread_crashes(self):
        from scripts import import_preview_worker

        class ThreadDB:
            def close(self):
                pass

        calls = 0
        calls_lock = threading.Lock()

        def run_once(db, *, worker_id):
            nonlocal calls
            with calls_lock:
                calls += 1
                if calls == 1:
                    raise RuntimeError("db connection died")
            return None

        with (
            patch("scripts.import_preview_worker.PipelineDB",
                  side_effect=lambda dsn: ThreadDB()),
            patch("scripts.import_preview_worker.run_once",
                  side_effect=run_once),
            patch("scripts.import_preview_worker.logger.exception"),
            patch("scripts.import_preview_worker.logger.error"),
        ):
            exit_code = import_preview_worker.run_threaded_workers(
                dsn="postgresql://example",
                worker_id="preview-test",
                worker_count=2,
                poll_interval=60.0,
            )

        self.assertEqual(exit_code, 1)

    def test_threaded_worker_treats_db_operational_error_as_transient(self):
        """A dead DB connection raised mid-poll must NOT kill the worker.

        Live failure mode (2026-05-20): PostgreSQL drops the
        preview-worker connection during long idle windows between
        jobs; libpq doesn't notice until the next send, so the next
        ``claim_next_import_preview_job`` raises
        ``psycopg2.OperationalError``. Previously this propagated out of
        ``worker_loop`` into the ``BaseException`` handler, which set
        ``stop`` and crashed the whole process with exit-code 1 — even
        though ``PipelineDB._execute`` now reconnects on subsequent
        calls. Defense in depth: the worker must catch the transient
        error, log it, back off, and keep polling.
        """
        import psycopg2
        from scripts import import_preview_worker

        class ThreadDB:
            def close(self):
                pass

        calls = 0
        calls_lock = threading.Lock()
        stop_holder: dict[str, Any] = {}

        def run_once(db, *, worker_id):
            nonlocal calls
            with calls_lock:
                calls += 1
                current = calls
            if current == 1:
                raise psycopg2.OperationalError(
                    "server closed the connection unexpectedly"
                )
            # On the second iteration, stop the workers so the test
            # terminates. We grab the live ``stop`` event via the
            # ``run_threaded_workers`` frame for visibility.
            stop = stop_holder.get("stop")
            if stop is not None:
                stop.set()
            return None

        # Capture the ``stop`` event from inside ``run_threaded_workers``
        # by monkeypatching ``threading.Event``.
        real_event = threading.Event

        def capturing_event():
            ev = real_event()
            stop_holder.setdefault("stop", ev)
            return ev

        with (
            patch("scripts.import_preview_worker.PipelineDB",
                  side_effect=lambda dsn: ThreadDB()),
            patch("scripts.import_preview_worker.run_once",
                  side_effect=run_once),
            patch("scripts.import_preview_worker.threading.Event",
                  side_effect=capturing_event),
            patch("scripts.import_preview_worker.logger.warning"),
            patch("scripts.import_preview_worker.logger.exception"),
            patch("scripts.import_preview_worker.logger.error"),
        ):
            exit_code = import_preview_worker.run_threaded_workers(
                dsn="postgresql://example",
                worker_id="preview-test",
                worker_count=1,
                poll_interval=0.01,
            )

        self.assertEqual(exit_code, 0)
        # We saw at least the transient raise + one post-recover poll.
        self.assertGreaterEqual(calls, 2)

    def test_main_requeues_running_preview_jobs_on_startup(self):
        """A preview job left in ``preview_status='running'`` by a dead
        worker process must be flipped back to ``waiting`` the moment
        ``main()`` runs — not after the 15-minute stale-recovery
        window. Mirrors the importer's startup self-heal.
        """
        from scripts import import_preview_worker

        db = FakePipelineDB()
        db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            dedupe_key=force_import_dedupe_key(7),
            payload=force_import_payload(
                download_log_id=7,
                failed_path="/tmp/failed",
                source_username="alice",
            ),
        )
        claimed = db.claim_next_import_preview_job(worker_id="dead-worker")
        assert claimed is not None
        self.assertEqual(claimed.preview_status, "running")

        argv = ["import_preview_worker.py", "--dsn", "postgresql://fake", "--once"]
        with (
            patch("sys.argv", argv),
            patch("scripts.import_preview_worker.PipelineDB",
                  side_effect=lambda dsn: db),
            patch("scripts.import_preview_worker.run_once", return_value=None),
            patch("scripts.import_preview_worker.logger.warning"),
        ):
            exit_code = import_preview_worker.main()

        self.assertEqual(exit_code, 0)
        recovered = db.get_import_job(claimed.id)
        assert recovered is not None
        self.assertEqual(recovered.preview_status, "waiting")
        self.assertIsNone(recovered.preview_worker_id)
        self.assertIsNone(recovered.preview_heartbeat_at)


class TestImportPreviewWorkerFrontGate(unittest.TestCase):
    """U1: worker short-circuits measurement when stored candidate evidence
    already passes the snapshot guard.

    Covers AE4 (re-claim of valid evidence skips measurement) for both
    force/manual and automation job types.
    """

    def _seed_evidence_for_job(
        self,
        db: FakePipelineDB,
        job_id: int,
        source_path: str,
    ) -> None:
        _seed_candidate_for_import_job(
            db, job_id,
            mb_release_id=f"mbid-frontgate-job-{job_id}",
            files=snapshot_audio_files(source_path),
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=245,
                avg_bitrate_kbps=256,
                median_bitrate_kbps=252,
                format="MP3 V0",
                spectral_grade="genuine",
            ),
            codec="mp3",
            container="mp3",
            storage_format="mp3 v0",
        )

    def _seed_evidence_for_download_log(
        self,
        db: FakePipelineDB,
        download_log_id: int,
        source_path: str,
    ) -> None:
        _seed_candidate_for_download_log(
            db, download_log_id,
            mb_release_id=f"mbid-frontgate-dl-{download_log_id}",
            files=snapshot_audio_files(source_path),
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=245,
                avg_bitrate_kbps=256,
                median_bitrate_kbps=252,
                format="MP3 V0",
                spectral_grade="genuine",
            ),
            codec="mp3",
            container="mp3",
            storage_format="mp3 v0",
        )

    def test_force_job_valid_evidence_skips_measurement(self):
        """AE4 force/manual: matching snapshot + valid evidence → no measurement."""
        from scripts import import_preview_worker

        with tempfile.TemporaryDirectory() as source:
            with open(os.path.join(source, "01.mp3"), "wb") as handle:
                handle.write(b"audio")
            db = FakePipelineDB()
            db.seed_request(make_request_row(id=42))
            download_log_id = db.log_download(42, outcome="rejected")
            db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                dedupe_key=force_import_dedupe_key(download_log_id),
                payload=force_import_payload(
                    download_log_id=download_log_id,
                    failed_path=source,
                    source_username="alice",
                ),
            )
            claimed = db.claim_next_import_preview_job(worker_id="preview")
            assert claimed is not None
            # Seed download_log_candidate evidence — force/manual path uses it.
            self._seed_evidence_for_download_log(db, download_log_id, source)

            with patch(
                "scripts.import_preview_worker.measure_and_persist_candidate_evidence",
            ) as preview, patch(
                "lib.measurement.measure_preimport_state",
            ) as preimport, patch(
                "lib.spectral_check.analyze_album",
            ) as spectral:
                updated = import_preview_worker.process_claimed_preview_job(
                    db,
                    claimed,
                )

        preview.assert_not_called()
        preimport.assert_not_called()
        spectral.assert_not_called()
        assert updated is not None
        self.assertEqual(updated.status, "queued")
        self.assertEqual(updated.preview_status, "evidence_ready")
        assert updated.preview_result is not None
        self.assertEqual(
            updated.preview_result.get("candidate_status"),
            "reused",
        )
        self.assertIsNotNone(updated.importable_at)

    def test_manual_job_valid_evidence_skips_measurement(self):
        """AE4 manual: matching snapshot + valid evidence → no measurement."""
        from scripts import import_preview_worker

        with tempfile.TemporaryDirectory() as source:
            with open(os.path.join(source, "01.mp3"), "wb") as handle:
                handle.write(b"audio")
            db = FakePipelineDB()
            db.seed_request(make_request_row(id=42))
            db.enqueue_import_job(
                IMPORT_JOB_MANUAL,
                request_id=42,
                dedupe_key=manual_import_dedupe_key(42, source),
                payload=manual_import_payload(failed_path=source),
            )
            claimed = db.claim_next_import_preview_job(worker_id="preview")
            assert claimed is not None
            # Manual jobs have no download_log; seed import_job_candidate.
            self._seed_evidence_for_job(db, claimed.id, source)

            with patch(
                "scripts.import_preview_worker.measure_and_persist_candidate_evidence",
            ) as preview, patch(
                "lib.measurement.measure_preimport_state",
            ) as preimport:
                updated = import_preview_worker.process_claimed_preview_job(
                    db,
                    claimed,
                )

        preview.assert_not_called()
        preimport.assert_not_called()
        assert updated is not None
        self.assertEqual(updated.preview_status, "evidence_ready")
        assert updated.preview_result is not None
        self.assertEqual(
            updated.preview_result.get("candidate_status"),
            "reused",
        )

    def test_automation_job_valid_evidence_skips_measurement_and_materialization(self):
        """AE4 automation: matching snapshot + valid evidence → no measurement.

        Crucially, no materialization either: the path-derivation helper must
        not invoke _materialize_processing_dir.
        """
        from scripts import import_preview_worker

        with tempfile.TemporaryDirectory() as staged:
            with open(os.path.join(staged, "01.flac"), "wb") as handle:
                handle.write(b"audio")
            db = FakePipelineDB()
            db.seed_request(make_request_row(
                id=42,
                status="downloading",
                active_download_state={
                    "filetype": "flac",
                    "enqueued_at": "2026-04-25T00:00:00+00:00",
                    "current_path": staged,
                    "files": [{
                        "username": "alice",
                        "filename": "Artist\\Album\\01.flac",
                        "file_dir": "Artist\\Album",
                        "size": 123,
                    }],
                },
            ))
            db.enqueue_import_job(
                IMPORT_JOB_AUTOMATION,
                request_id=42,
                dedupe_key=automation_import_dedupe_key(42),
                payload={},
            )
            claimed = db.claim_next_import_preview_job(worker_id="preview")
            assert claimed is not None
            self._seed_evidence_for_job(db, claimed.id, staged)

            with patch(
                "scripts.import_preview_worker.measure_and_persist_candidate_evidence",
            ) as preview, patch(
                "lib.measurement.measure_preimport_state",
            ) as preimport, patch(
                "lib.download._materialize_processing_dir",
            ) as materialize:
                updated = import_preview_worker.process_claimed_preview_job(
                    db,
                    claimed,
                )

        preview.assert_not_called()
        preimport.assert_not_called()
        materialize.assert_not_called()
        assert updated is not None
        self.assertEqual(updated.preview_status, "evidence_ready")
        assert updated.preview_result is not None
        self.assertEqual(
            updated.preview_result.get("candidate_status"),
            "reused",
        )

    def test_missing_evidence_falls_through_to_full_measurement(self):
        """No evidence row → worker runs full preview measurement (legacy path)."""
        from scripts import import_preview_worker

        with tempfile.TemporaryDirectory() as source:
            with open(os.path.join(source, "01.mp3"), "wb") as handle:
                handle.write(b"audio")
            db = FakePipelineDB()
            db.seed_request(make_request_row(id=42))
            download_log_id = db.log_download(42, outcome="rejected")
            db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                dedupe_key=force_import_dedupe_key(download_log_id),
                payload=force_import_payload(
                    download_log_id=download_log_id,
                    failed_path=source,
                    source_username="alice",
                ),
            )
            claimed = db.claim_next_import_preview_job(worker_id="preview")
            assert claimed is not None
            # No evidence seeded → front-gate misses → measurement runs.

            # Post-U5: worker-mode preview emits ``evidence_ready`` (not the
            # legacy ``would_import``); the importer reads the evidence and
            # decides.
            preview_result = ImportPreviewResult(
                mode="path",
                verdict="evidence_ready",
                decision="import",
                reason="import",
                stage_chain=["preview:import"],
                source_path=source,
            )

            def fake_preview(*args: Any, **kwargs: Any) -> ImportPreviewResult:
                # Simulate production: preview persists candidate evidence
                # and wires the FK chain that the front-gate reads from.
                _seed_candidate_for_download_log(
                    db, download_log_id,
                    mb_release_id="mbid-missing-falls-through",
                    files=snapshot_audio_files(source),
                )
                return preview_result

            with patch(
                "scripts.import_preview_worker.measure_and_persist_candidate_evidence",
                side_effect=fake_preview,
            ) as preview:
                updated = import_preview_worker.process_claimed_preview_job(
                    db,
                    claimed,
                )

        # Front-gate misses (no evidence) → preview is called.
        preview.assert_called_once()
        assert updated is not None
        self.assertEqual(updated.preview_status, "evidence_ready")
        # Provenance reflects the measured path, not the reused path.
        assert updated.preview_result is not None
        self.assertNotEqual(
            updated.preview_result.get("candidate_status"),
            "reused",
        )

    def test_snapshot_mismatch_falls_through_to_full_measurement(self):
        """Stale snapshot → measurement runs; new evidence replaces stale row."""
        from scripts import import_preview_worker

        with tempfile.TemporaryDirectory() as source:
            with open(os.path.join(source, "01.mp3"), "wb") as handle:
                handle.write(b"audio")
            db = FakePipelineDB()
            db.seed_request(make_request_row(id=42))
            download_log_id = db.log_download(42, outcome="rejected")
            db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                dedupe_key=force_import_dedupe_key(download_log_id),
                payload=force_import_payload(
                    download_log_id=download_log_id,
                    failed_path=source,
                    source_username="alice",
                ),
            )
            claimed = db.claim_next_import_preview_job(worker_id="preview")
            assert claimed is not None
            # Seed evidence with files that don't match the on-disk snapshot.
            from lib.quality import AlbumQualityEvidenceFile
            _seed_candidate_for_download_log(
                db, download_log_id,
                mb_release_id="mbid-stale",
                files=[AlbumQualityEvidenceFile(
                    relative_path="stale.mp3",
                    size_bytes=999,
                    mtime_ns=1,
                    extension="mp3",
                    container="mp3",
                    codec="mp3",
                )],
            )

            # Post-U5: worker-mode preview emits ``evidence_ready``.
            preview_result = ImportPreviewResult(
                mode="path",
                verdict="evidence_ready",
                decision="import",
                reason="import",
                stage_chain=["preview:import"],
                source_path=source,
            )

            def fake_preview(*args: Any, **kwargs: Any) -> ImportPreviewResult:
                # Simulate production: preview re-measures and persists fresh
                # evidence with the actual on-disk snapshot, rewiring the FK.
                _seed_candidate_for_download_log(
                    db, download_log_id,
                    mb_release_id="mbid-fresh",
                    files=snapshot_audio_files(source),
                )
                return preview_result

            with patch(
                "scripts.import_preview_worker.measure_and_persist_candidate_evidence",
                side_effect=fake_preview,
            ) as preview:
                updated = import_preview_worker.process_claimed_preview_job(
                    db,
                    claimed,
                )

        # Snapshot mismatch → front-gate misses → preview ran.
        preview.assert_called_once()
        assert updated is not None
        self.assertEqual(updated.preview_status, "evidence_ready")
        # The stale evidence row was replaced — the FK now points at fresh
        # content-addressed evidence.
        evidence_id = db.get_download_log_candidate_evidence_id(download_log_id)
        self.assertIsNotNone(evidence_id)
        evidence = db.load_album_quality_evidence_by_id(evidence_id)
        assert evidence is not None
        self.assertEqual(len(evidence.files), 1)
        self.assertEqual(evidence.files[0].relative_path, "01.mp3")


class TestYoutubeImportJobType(unittest.TestCase):
    """Constant + helper coverage for the YT-rescue ``youtube_import`` job_type.

    Covers U2's import_queue extensions:
    - ``IMPORT_JOB_YOUTUBE`` is registered in ``IMPORT_JOB_TYPES``
    - ``youtube_import_dedupe_key`` is keyed on the download_log id
    - ``youtube_import_payload`` produces the {staged_path, request_id, browse_id}
      shape the U9 dispatcher consumes
    - ``validate_payload`` enforces all three fields are present and typed
    """

    def test_constant_is_in_registered_job_types(self):
        from lib.import_queue import IMPORT_JOB_TYPES, IMPORT_JOB_YOUTUBE
        self.assertIn(IMPORT_JOB_YOUTUBE, IMPORT_JOB_TYPES)
        self.assertEqual(IMPORT_JOB_YOUTUBE, "youtube_import")

    def test_validate_job_type_accepts_youtube_import(self):
        from lib.import_queue import validate_job_type, IMPORT_JOB_YOUTUBE
        self.assertEqual(
            validate_job_type(IMPORT_JOB_YOUTUBE), IMPORT_JOB_YOUTUBE,
        )

    def test_youtube_import_payload_roundtrip(self):
        from lib.import_queue import youtube_import_payload, validate_payload, IMPORT_JOB_YOUTUBE
        payload = youtube_import_payload(
            staged_path="/Incoming/auto-import/Artist - Album",
            request_id=42,
            browse_id="MPREb_abc",
        )
        self.assertEqual(payload, {
            "staged_path": "/Incoming/auto-import/Artist - Album",
            "request_id": 42,
            "browse_id": "MPREb_abc",
        })
        # validate_payload passes through unchanged for a valid payload.
        validated = validate_payload(IMPORT_JOB_YOUTUBE, payload)
        self.assertEqual(validated, payload)

    def test_youtube_import_payload_coerces_request_id_to_int(self):
        from lib.import_queue import youtube_import_payload
        payload = youtube_import_payload(
            staged_path="/Incoming/auto-import/Artist - Album",
            request_id=42,  # already int
            browse_id="MPREb_abc",
            download_log_id=99,
        )
        self.assertIsInstance(payload["request_id"], int)
        self.assertEqual(payload["download_log_id"], 99)

    def test_validate_payload_youtube_rejects_missing_staged_path(self):
        from lib.import_queue import validate_payload, IMPORT_JOB_YOUTUBE
        with self.assertRaises(ValueError):
            validate_payload(IMPORT_JOB_YOUTUBE, {
                "request_id": 42, "browse_id": "MPREb_abc",
            })

    def test_validate_payload_youtube_rejects_empty_staged_path(self):
        from lib.import_queue import validate_payload, IMPORT_JOB_YOUTUBE
        with self.assertRaises(ValueError):
            validate_payload(IMPORT_JOB_YOUTUBE, {
                "staged_path": "",
                "request_id": 42,
                "browse_id": "MPREb_abc",
            })

    def test_validate_payload_youtube_rejects_missing_request_id(self):
        from lib.import_queue import validate_payload, IMPORT_JOB_YOUTUBE
        with self.assertRaises(ValueError):
            validate_payload(IMPORT_JOB_YOUTUBE, {
                "staged_path": "/Incoming/auto-import/x",
                "browse_id": "MPREb_abc",
            })

    def test_validate_payload_youtube_rejects_non_int_request_id(self):
        from lib.import_queue import validate_payload, IMPORT_JOB_YOUTUBE
        with self.assertRaises(ValueError):
            validate_payload(IMPORT_JOB_YOUTUBE, {
                "staged_path": "/Incoming/auto-import/x",
                "request_id": "42",  # str, not int
                "browse_id": "MPREb_abc",
            })

    def test_validate_payload_youtube_rejects_missing_browse_id(self):
        from lib.import_queue import validate_payload, IMPORT_JOB_YOUTUBE
        with self.assertRaises(ValueError):
            validate_payload(IMPORT_JOB_YOUTUBE, {
                "staged_path": "/Incoming/auto-import/x",
                "request_id": 42,
            })

    def test_validate_payload_youtube_rejects_non_int_download_log_id(self):
        from lib.import_queue import validate_payload, IMPORT_JOB_YOUTUBE
        with self.assertRaises(ValueError):
            validate_payload(IMPORT_JOB_YOUTUBE, {
                "staged_path": "/Incoming/auto-import/x",
                "request_id": 42,
                "browse_id": "MPREb_abc",
                "download_log_id": "99",
            })

    def test_dedupe_key_uses_download_log_id(self):
        from lib.import_queue import youtube_import_dedupe_key
        self.assertEqual(
            youtube_import_dedupe_key(7),
            "youtube_import:download_log:7",
        )
        # Same id ⇒ same key (idempotency).
        self.assertEqual(
            youtube_import_dedupe_key(7), youtube_import_dedupe_key(7),
        )
        # Different id ⇒ different key.
        self.assertNotEqual(
            youtube_import_dedupe_key(7), youtube_import_dedupe_key(8),
        )


class TestExecuteYoutubeImportJob(unittest.TestCase):
    """U9: importer dispatcher for ``youtube_import`` job_type.

    Covers AE7 (happy-path import to terminal state), AE8 (long-tail
    rescue audit chain), AE9 (rescue from ``manual``), the preview-worker
    front-gate path-resolution divergence, no-cooldown-leakage, and
    payload type-validation.

    Test shape mirrors ``TestImporterWorker``: drive the production
    ``importer.process_claimed_job`` entry point with a ``FakePipelineDB``,
    seed a queued YT job, mark it importable so the importer can claim it,
    and patch the leaf seam (``lib.download.process_completed_album``) so
    we can assert dispatcher behaviour without exercising the full beets
    pipeline (which is covered by its own integration slices).
    """

    def _mark_importable(self, db: FakePipelineDB, job: Any) -> Any:
        updated = db.mark_import_job_preview_importable(
            job.id,
            preview_result={"verdict": "would_import"},
            message="ready",
        )
        assert updated is not None
        return updated

    def _enqueue_youtube_job(
        self,
        db: FakePipelineDB,
        *,
        request_id: int,
        staged_path: str,
        browse_id: str = "MPREb_abc",
        download_log_id: int = 1,
    ) -> Any:
        from lib.import_queue import (
            youtube_import_dedupe_key,
            youtube_import_payload,
        )
        return db.enqueue_import_job(
            IMPORT_JOB_YOUTUBE,
            request_id=request_id,
            dedupe_key=youtube_import_dedupe_key(download_log_id),
            payload=youtube_import_payload(
                staged_path=staged_path,
                request_id=request_id,
                browse_id=browse_id,
                download_log_id=download_log_id,
            ),
        )

    def _claim(self, db: FakePipelineDB) -> Any:
        claimed = db.claim_next_import_job(worker_id="worker")
        assert claimed is not None
        return claimed

    def test_happy_path_drives_through_pipeline_and_returns_success(self):
        """AE7: a YT job dispatched through importer.process_claimed_job
        runs the existing per-job pipeline (process_completed_album)
        with the staged path coming from the payload, NOT from
        active_download_state."""
        from scripts import importer

        with tempfile.TemporaryDirectory() as staged:
            with open(os.path.join(staged, "01.opus"), "wb") as fp:
                fp.write(b"audio")

            db = FakePipelineDB()
            db.seed_request(make_request_row(
                id=42,
                status="wanted",
                # No active_download_state — YT path must NOT depend on it.
                active_download_state=None,
                artist_name="Test Artist",
                album_title="Test Album",
                mb_release_id="mbid-yt-happy",
            ))
            job = self._enqueue_youtube_job(
                db, request_id=42, staged_path=staged, download_log_id=11,
            )
            self._mark_importable(db, job)
            claimed = self._claim(db)

            with patch(
                "lib.download.process_completed_album",
                return_value=DispatchOutcome(True, "Imported by dispatch"),
            ) as proc:
                updated = importer.process_claimed_job(
                    cast(Any, db),
                    claimed,
                    ctx=object(),
                )

        # Pipeline invoked exactly once with the import_job_id forwarded.
        proc.assert_called_once()
        self.assertEqual(proc.call_args.kwargs["import_job_id"], job.id)
        # The GrabListEntry passed in is sourced from the request row +
        # YT payload's staged_path, not from active_download_state.
        entry_arg = proc.call_args.args[0]
        self.assertEqual(entry_arg.import_folder, staged)
        self.assertEqual(entry_arg.db_request_id, 42)
        self.assertEqual([f.filename for f in entry_arg.files], ["01.opus"])
        self.assertEqual([f.username for f in entry_arg.files], [""])
        # Terminal queue state reflects the DispatchOutcome.
        assert updated is not None
        self.assertEqual(updated.status, "completed")
        self.assertEqual(updated.message, "Imported by dispatch")

    def test_youtube_staged_audio_manifest_uses_real_files(self):
        """Real dispatcher + process_completed_album reaches beets reject
        without the manifest guard classifying staged YT audio as
        untracked_audio.
        """
        from scripts import importer

        with tempfile.TemporaryDirectory() as tmpdir:
            root = os.path.abspath(tmpdir)
            staged = os.path.join(root, "yt-staged")
            os.makedirs(staged)
            for name in ("01.opus", "02.opus"):
                with open(os.path.join(staged, name), "wb") as fp:
                    fp.write(b"audio")

            db = FakePipelineDB()
            db.seed_request(make_request_row(
                id=42,
                status="wanted",
                active_download_state=None,
                artist_name="Test Artist",
                album_title="Test Album",
                mb_release_id="mbid-yt-real-manifest",
                search_filetype_override="opus",
            ))
            job = self._enqueue_youtube_job(
                db, request_id=42, staged_path=staged, download_log_id=110,
            )
            self._mark_importable(db, job)
            claimed = self._claim(db)
            cfg = CratediggerConfig(
                beets_validation_enabled=True,
                beets_harness_path="/unused",
                beets_staging_dir=os.path.join(root, "Incoming"),
                slskd_download_dir=os.path.join(root, "slskd"),
            )
            ctx = make_ctx_with_fake_db(db, cfg=cfg)

            with patch(
                "lib.beets.beets_validate",
                return_value=ValidationResult(
                    valid=False,
                    scenario="high_distance",
                    detail="distance=1.0",
                    target_mbid="mbid-yt-real-manifest",
                ),
            ):
                updated = importer.process_claimed_job(
                    cast(Any, db),
                    claimed,
                    ctx=ctx,
                )

            assert updated is not None
            self.assertEqual(updated.status, "failed")
            source = ctx.pipeline_db_source
            self.assertEqual(len(source.reject_and_requeue_calls), 1)
            rejected = source.reject_and_requeue_calls[0]["bv_result"]
            self.assertEqual(rejected.scenario, "high_distance")
            self.assertNotEqual(rejected.scenario, "untracked_audio")
            self.assertFalse(
                os.path.exists(
                    os.path.join(root, "failed_imports", "untracked_audio")
                )
            )

    def test_happy_path_finalizes_request_to_imported_via_rescue(self):
        """AE7: when process_completed_album returns True (legacy non-
        DispatchOutcome path), the dispatcher reports success and the
        request row remains untouched here — the actual status flip
        happens inside the dispatch path via finalize_request →
        mark_imported_with_rescue. We assert success-mapping without
        re-deriving the status flip (which is covered separately by
        TestRescueAuditChain below)."""
        from scripts import importer

        with tempfile.TemporaryDirectory() as staged:
            db = FakePipelineDB()
            db.seed_request(make_request_row(
                id=42,
                status="wanted",
                active_download_state=None,
                mb_release_id="mbid-yt-true",
            ))
            job = self._enqueue_youtube_job(
                db, request_id=42, staged_path=staged, download_log_id=12,
            )
            self._mark_importable(db, job)
            claimed = self._claim(db)

            with patch(
                "lib.download.process_completed_album",
                return_value=True,
            ):
                updated = importer.process_claimed_job(
                    cast(Any, db),
                    claimed,
                    ctx=object(),
                )

        assert updated is not None
        self.assertEqual(updated.status, "completed")
        self.assertEqual(updated.message, "YouTube import processing completed")

    def test_rescue_audit_chain_wanted_to_imported_via_finalize_request(self):
        """AE8: a YT import on a previously-unfindable request populates
        ``rescued_at`` + ``prior_unfindable_category`` atomically. The
        dispatcher invokes the pipeline; the pipeline invokes
        ``finalize_request`` which routes to ``mark_imported_with_rescue``.

        We drive the rescue capture seam directly by having the patched
        pipeline call ``finalize_request(to_imported)`` against the fake DB,
        which mirrors what the production pipeline does inside
        ``dispatch_import_from_db`` on import success.
        """
        from scripts import importer
        from lib import transitions

        with tempfile.TemporaryDirectory() as staged:
            db = FakePipelineDB()
            db.seed_request(make_request_row(
                id=42,
                status="wanted",
                active_download_state=None,
                mb_release_id="mbid-yt-rescue",
                unfindable_category="wrong_pressing_available",
                unfindable_categorised_at=datetime(
                    2026, 5, 1, tzinfo=timezone.utc),
            ))
            job = self._enqueue_youtube_job(
                db, request_id=42, staged_path=staged, download_log_id=13,
            )
            self._mark_importable(db, job)
            claimed = self._claim(db)

            def _run_real_finalize(*args: Any, **kwargs: Any) -> DispatchOutcome:
                # Mirror the production seam: dispatch_import_from_db
                # would call finalize_request(to_imported) on auto-import
                # success. That call routes to mark_imported_with_rescue.
                transitions.finalize_request(
                    cast(Any, db),
                    42,
                    transitions.RequestTransition.to_imported(
                        from_status="wanted",
                    ),
                )
                return DispatchOutcome(True, "Imported")

            with patch(
                "lib.download.process_completed_album",
                side_effect=_run_real_finalize,
            ):
                updated = importer.process_claimed_job(
                    cast(Any, db),
                    claimed,
                    ctx=object(),
                )

        # The job completed successfully…
        assert updated is not None
        self.assertEqual(updated.status, "completed")
        # …and the request row carries the long-tail-rescue audit chain.
        row = db.get_request(42)
        assert row is not None
        self.assertEqual(row["status"], "imported")
        self.assertIsNotNone(row["rescued_at"])
        self.assertEqual(
            row["prior_unfindable_category"], "wrong_pressing_available")
        # Current category is cleared (the rescue IS the resolution).
        self.assertIsNone(row["unfindable_category"])

    def test_rescue_from_manual_transitions_manual_to_imported(self):
        """AE9: a request started in ``manual`` status transitions
        ``manual → imported`` through the same single source-agnostic
        write site (``mark_imported_with_rescue``)."""
        from scripts import importer
        from lib import transitions

        with tempfile.TemporaryDirectory() as staged:
            db = FakePipelineDB()
            db.seed_request(make_request_row(
                id=42,
                status="manual",
                active_download_state=None,
                mb_release_id="mbid-yt-from-manual",
                unfindable_category="album_absent_artist_present",
                unfindable_categorised_at=datetime(
                    2026, 5, 1, tzinfo=timezone.utc),
            ))
            job = self._enqueue_youtube_job(
                db, request_id=42, staged_path=staged, download_log_id=14,
            )
            self._mark_importable(db, job)
            claimed = self._claim(db)

            def _run_real_finalize(*args: Any, **kwargs: Any) -> DispatchOutcome:
                transitions.finalize_request(
                    cast(Any, db),
                    42,
                    transitions.RequestTransition.to_imported(
                        from_status="manual",
                    ),
                )
                return DispatchOutcome(True, "Imported from manual")

            with patch(
                "lib.download.process_completed_album",
                side_effect=_run_real_finalize,
            ):
                updated = importer.process_claimed_job(
                    cast(Any, db),
                    claimed,
                    ctx=object(),
                )

        assert updated is not None
        self.assertEqual(updated.status, "completed")
        row = db.get_request(42)
        assert row is not None
        self.assertEqual(row["status"], "imported")
        self.assertIsNotNone(row["rescued_at"])
        self.assertEqual(
            row["prior_unfindable_category"], "album_absent_artist_present")

    def test_no_cooldown_leakage_on_wrong_match_reject(self):
        """The dispatcher running through the wrong-matches reject path
        does not denylist any user or apply any cooldown — YT produces
        no peer to attribute failures to.
        """
        from scripts import importer

        with tempfile.TemporaryDirectory() as staged:
            db = FakePipelineDB()
            db.seed_request(make_request_row(
                id=42,
                status="wanted",
                active_download_state=None,
                mb_release_id="mbid-yt-reject",
            ))
            job = self._enqueue_youtube_job(
                db, request_id=42, staged_path=staged, download_log_id=15,
            )
            self._mark_importable(db, job)
            claimed = self._claim(db)

            # Simulate a wrong-matches reject from the pipeline.
            with patch(
                "lib.download.process_completed_album",
                return_value=DispatchOutcome(
                    False, "Rejected: high_distance"),
            ):
                updated = importer.process_claimed_job(
                    cast(Any, db),
                    claimed,
                    ctx=object(),
                )

        assert updated is not None
        self.assertEqual(updated.status, "failed")
        # No slskd peer means nothing to denylist and nothing to cool.
        self.assertEqual(db.denylist, [])
        self.assertEqual(db.user_cooldowns, {})

    def test_no_cooldown_leakage_on_quality_pipeline_reject(self):
        """Same invariant on the quality-pipeline reject path: empty
        files list ⇒ no usernames to attribute ⇒ denylist + cooldowns
        remain untouched."""
        from scripts import importer

        with tempfile.TemporaryDirectory() as staged:
            db = FakePipelineDB()
            db.seed_request(make_request_row(
                id=42,
                status="wanted",
                active_download_state=None,
                mb_release_id="mbid-yt-quality-reject",
            ))
            job = self._enqueue_youtube_job(
                db, request_id=42, staged_path=staged, download_log_id=16,
            )
            self._mark_importable(db, job)
            claimed = self._claim(db)

            with patch(
                "lib.download.process_completed_album",
                return_value=DispatchOutcome(
                    False,
                    "Quality pipeline rejected",
                    code=DISPATCH_CODE_QUALITY_PIPELINE_REJECTED,
                ),
            ):
                updated = importer.process_claimed_job(
                    cast(Any, db),
                    claimed,
                    ctx=object(),
                )

        assert updated is not None
        self.assertEqual(updated.status, "failed")
        self.assertEqual(db.denylist, [])
        self.assertEqual(db.user_cooldowns, {})

    def test_dispatcher_does_not_read_or_write_active_download_state(self):
        """KTD1: the YT dispatcher never touches active_download_state.
        The path source-of-truth is the payload's ``staged_path``.

        Seed a row with a DIFFERENT active_download_state.current_path
        than the payload's staged_path; assert the pipeline gets the
        payload's path, and assert the row's active_download_state is
        unchanged after dispatch.
        """
        from scripts import importer

        with tempfile.TemporaryDirectory() as staged:
            db = FakePipelineDB()
            unrelated_state = {
                "filetype": "flac",
                "enqueued_at": "2026-04-25T00:00:00+00:00",
                "current_path": "/some/unrelated/slskd/path",
                "files": [{
                    "username": "alice",
                    "filename": "Artist\\Album\\01.flac",
                    "file_dir": "Artist\\Album",
                    "size": 123,
                }],
            }
            db.seed_request(make_request_row(
                id=42,
                status="wanted",
                # Even if some unrelated automation state happens to be
                # populated, the YT path must ignore it.
                active_download_state=unrelated_state,
                mb_release_id="mbid-yt-ktd1",
            ))
            job = self._enqueue_youtube_job(
                db, request_id=42, staged_path=staged, download_log_id=17,
            )
            self._mark_importable(db, job)
            claimed = self._claim(db)

            with patch(
                "lib.download.process_completed_album",
                return_value=DispatchOutcome(True, "ok"),
            ) as proc:
                importer.process_claimed_job(
                    cast(Any, db),
                    claimed,
                    ctx=object(),
                )

        # The entry handed to the pipeline carries the YT staged_path,
        # NOT the unrelated active_download_state current_path.
        entry_arg = proc.call_args.args[0]
        self.assertEqual(entry_arg.import_folder, staged)
        self.assertEqual(entry_arg.files, [])
        # And the row's active_download_state is untouched.
        row = db.get_request(42)
        assert row is not None
        self.assertEqual(
            row.get("active_download_state"), unrelated_state)

    def test_missing_request_returns_failed_dispatch_outcome(self):
        from scripts import importer

        db = FakePipelineDB()
        # Enqueue against a request_id that has no row.
        job = self._enqueue_youtube_job(
            db, request_id=999, staged_path="/Incoming/auto-import/x",
            download_log_id=18,
        )
        self._mark_importable(db, job)
        claimed = self._claim(db)

        updated = importer.process_claimed_job(
            cast(Any, db),
            claimed,
            ctx=object(),
        )

        assert updated is not None
        self.assertEqual(updated.status, "failed")
        self.assertIn("not found", str(updated.message))

    def test_missing_request_id_returns_failed_dispatch_outcome(self):
        from scripts import importer
        from lib.import_queue import ImportJob

        # Construct an ImportJob with request_id=None directly — this is
        # a defensive guard for a path the production codepaths shouldn't
        # produce (the YT submit guards block it upstream), but the
        # dispatcher must still fail-fast rather than crash.
        db = FakePipelineDB()
        job = ImportJob.from_row({
            "id": 1,
            "job_type": IMPORT_JOB_YOUTUBE,
            "status": "queued",
            "request_id": None,
            "dedupe_key": "youtube_import:download_log:99",
            "payload": {
                "staged_path": "/Incoming/auto-import/x",
                "request_id": 1,
                "browse_id": "MPREb_abc",
            },
        })

        outcome = importer.execute_youtube_import_job(
            cast(Any, db), job, ctx=object(),
        )

        self.assertFalse(outcome.success)
        self.assertIn("request_id", outcome.message)

    def test_malformed_payload_returns_failed_dispatch_outcome(self):
        """Payload type-validation: missing ``staged_path`` raises
        ``msgspec.ValidationError`` at the wire seam and the dispatcher
        surfaces it as a failed ``DispatchOutcome`` rather than crashing
        the worker.
        """
        from scripts import importer
        from lib.import_queue import ImportJob

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="wanted"))
        # Build a job with a malformed payload (missing staged_path).
        # We construct ImportJob directly because the queue validator
        # would otherwise reject this payload — we want the dispatcher
        # to handle the case where a malformed row somehow lands.
        job = ImportJob.from_row({
            "id": 1,
            "job_type": IMPORT_JOB_YOUTUBE,
            "status": "queued",
            "request_id": 42,
            "dedupe_key": "youtube_import:download_log:42",
            "payload": {
                "request_id": 42,
                "browse_id": "MPREb_abc",
                # staged_path missing
            },
        })

        outcome = importer.execute_youtube_import_job(
            cast(Any, db), job, ctx=object(),
        )

        self.assertFalse(outcome.success)
        self.assertIn("malformed", outcome.message.lower())

    def test_malformed_payload_with_wrong_type_for_request_id(self):
        """Wrong type at the wire seam (request_id is str, not int) is
        a ValidationError, not a silent coerce."""
        from scripts import importer
        from lib.import_queue import ImportJob

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="wanted"))
        job = ImportJob.from_row({
            "id": 1,
            "job_type": IMPORT_JOB_YOUTUBE,
            "status": "queued",
            "request_id": 42,
            "dedupe_key": "youtube_import:download_log:43",
            "payload": {
                "staged_path": "/Incoming/auto-import/x",
                "request_id": "42",  # wrong type
                "browse_id": "MPREb_abc",
            },
        })

        outcome = importer.execute_youtube_import_job(
            cast(Any, db), job, ctx=object(),
        )

        self.assertFalse(outcome.success)
        self.assertIn("malformed", outcome.message.lower())


class TestFrontGateSourcePathYoutubeImport(unittest.TestCase):
    """U9: preview-worker front-gate divergence between job_types.

    ``_front_gate_source_path`` is the cheap path-derivation helper the
    preview worker uses to test stored candidate evidence's snapshot
    against the current source location before deciding whether to skip
    measurement. For YT jobs the path comes from the payload; for
    automation jobs it comes from ``active_download_state``. The two
    branches are independent and the YT branch never reads
    ``active_download_state`` (KTD1).

    Also covers ``_preview_input`` parity — the worker can fall through
    to full measurement for a YT job by reading the same payload
    seam (not active_download_state).
    """

    def test_youtube_job_returns_payload_staged_path(self):
        from scripts import import_preview_worker
        from lib.import_queue import (
            ImportJob,
            youtube_import_payload,
            youtube_import_dedupe_key,
        )

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="wanted"))
        job = ImportJob.from_row({
            "id": 1,
            "job_type": IMPORT_JOB_YOUTUBE,
            "status": "queued",
            "request_id": 42,
            "dedupe_key": youtube_import_dedupe_key(99),
            "payload": youtube_import_payload(
                staged_path="/Incoming/auto-import/Artist - Album",
                request_id=42,
                browse_id="MPREb_abc",
            ),
        })

        result = import_preview_worker._front_gate_source_path(
            cast(Any, db), job,
        )

        self.assertEqual(result, "/Incoming/auto-import/Artist - Album")

    def test_youtube_job_does_not_read_active_download_state(self):
        """KTD1 at the front-gate: even if active_download_state has a
        current_path populated, the YT branch returns the payload's
        staged_path."""
        from scripts import import_preview_worker
        from lib.import_queue import (
            ImportJob,
            youtube_import_payload,
            youtube_import_dedupe_key,
        )

        db = FakePipelineDB()
        # Seed a row with active_download_state pointing somewhere else.
        db.seed_request(make_request_row(
            id=42,
            status="wanted",
            active_download_state={
                "filetype": "flac",
                "enqueued_at": "2026-04-25T00:00:00+00:00",
                "current_path": "/totally/different/path",
                "files": [],
            },
        ))
        job = ImportJob.from_row({
            "id": 1,
            "job_type": IMPORT_JOB_YOUTUBE,
            "status": "queued",
            "request_id": 42,
            "dedupe_key": youtube_import_dedupe_key(99),
            "payload": youtube_import_payload(
                staged_path="/Incoming/auto-import/Artist - Album",
                request_id=42,
                browse_id="MPREb_abc",
            ),
        })

        result = import_preview_worker._front_gate_source_path(
            cast(Any, db), job,
        )

        # Returns the payload path, not the active_download_state path.
        self.assertEqual(result, "/Incoming/auto-import/Artist - Album")

    def test_automation_branch_is_independent_of_youtube_branch(self):
        """Sanity: the automation branch still reads active_download_state.
        The YT branch was added without altering automation behaviour."""
        from scripts import import_preview_worker
        from lib.import_queue import ImportJob, automation_import_dedupe_key

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            status="downloading",
            active_download_state={
                "filetype": "flac",
                "enqueued_at": "2026-04-25T00:00:00+00:00",
                "current_path": "/slskd/Test Artist - Test Album",
                "files": [{
                    "username": "alice",
                    "filename": "Artist\\Album\\01.flac",
                    "file_dir": "Artist\\Album",
                    "size": 123,
                }],
            },
        ))
        job = ImportJob.from_row({
            "id": 1,
            "job_type": IMPORT_JOB_AUTOMATION,
            "status": "queued",
            "request_id": 42,
            "dedupe_key": automation_import_dedupe_key(42),
            "payload": {},
        })

        # The automation branch derives from active_download_state +
        # canonical path computation. Patch out the canonical
        # derivation to a deterministic value (we just want to prove
        # the branch was taken, not retest the derivation logic).
        with patch(
            "scripts.import_preview_worker.derive_canonical_import_folder",
            return_value="/derived/from/active_download_state",
        ) as derive:
            result = import_preview_worker._front_gate_source_path(
                cast(Any, db), job,
            )

        derive.assert_called_once()
        self.assertEqual(result, "/derived/from/active_download_state")

    def test_youtube_job_malformed_payload_falls_through(self):
        """Malformed YT payload → front-gate returns None so the worker
        falls through to full measurement (rather than crashing)."""
        from scripts import import_preview_worker
        from lib.import_queue import ImportJob

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="wanted"))
        job = ImportJob.from_row({
            "id": 1,
            "job_type": IMPORT_JOB_YOUTUBE,
            "status": "queued",
            "request_id": 42,
            "dedupe_key": "youtube_import:download_log:99",
            # Missing staged_path.
            "payload": {"request_id": 42, "browse_id": "MPREb_abc"},
        })

        result = import_preview_worker._front_gate_source_path(
            cast(Any, db), job,
        )

        self.assertIsNone(result)

    def test_preview_input_uses_payload_staged_path_for_youtube(self):
        """The ``_preview_input`` helper (the slow-path measurement seam)
        also reads the YT payload, not active_download_state."""
        from scripts import import_preview_worker
        from lib.import_queue import (
            ImportJob,
            youtube_import_payload,
            youtube_import_dedupe_key,
        )

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="wanted"))
        job = ImportJob.from_row({
            "id": 1,
            "job_type": IMPORT_JOB_YOUTUBE,
            "status": "queued",
            "request_id": 42,
            "dedupe_key": youtube_import_dedupe_key(99),
            "payload": youtube_import_payload(
                staged_path="/Incoming/auto-import/Artist - Album",
                request_id=42,
                browse_id="MPREb_abc",
            ),
        })

        result = import_preview_worker._preview_input(
            cast(Any, db), job,
        )

        self.assertEqual(result["path"], "/Incoming/auto-import/Artist - Album")
        self.assertEqual(result["request_id"], 42)
        # The measurement core has no peer notion — no source_username key.
        self.assertNotIn("source_username", result)
        self.assertFalse(result["force"])

    def test_preview_input_raises_on_malformed_youtube_payload(self):
        from scripts import import_preview_worker
        from lib.import_queue import ImportJob

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="wanted"))
        job = ImportJob.from_row({
            "id": 1,
            "job_type": IMPORT_JOB_YOUTUBE,
            "status": "queued",
            "request_id": 42,
            "dedupe_key": "youtube_import:download_log:99",
            "payload": {"request_id": 42, "browse_id": "MPREb_abc"},
        })

        with self.assertRaises(ValueError) as ctx:
            import_preview_worker._preview_input(cast(Any, db), job)

        self.assertIn("malformed", str(ctx.exception).lower())
