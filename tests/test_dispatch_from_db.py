"""Tests for dispatch_import_from_db — force-import through the real pipeline.

Orchestration tests use FakePipelineDB to assert domain state (request status,
log rows, denylist). Seam tests verify argv/config wiring.
"""

import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import msgspec

from lib import transitions
from lib.config import CratediggerConfig
from lib.dispatch.quality_gate import QualityGatePlan
from lib.import_evidence import (
    ActionEvidenceProvenance,
    CandidateEvidenceActionResult,
)
from lib.import_queue import IMPORT_JOB_FORCE
from lib.quality import AudioQualityMeasurement, ImportResult
from lib.quality_evidence import snapshot_audio_files
from tests.helpers import (
    RecordingQualityGate,
    make_album_quality_evidence,
    make_import_result,
    make_request_row,
    noop_quality_gate,
    patch_dispatch_externals,
)
from tests.fakes import FakePipelineDB


# Migration 021 helpers — seed evidence and wire the FK chain that
# production reads through (download_log.candidate_evidence_id,
# import_jobs.candidate_evidence_id, album_requests.current_evidence_id).
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


def _mock_beets_db_for_dispatch():
    """Mock BeetsDB so ``_load_evidence_import_gate`` can fetch a stub
    ``AlbumInfo`` without a real beets connection.

    ``album_path=None`` makes ``ensure_current_evidence_for_action`` skip
    the audio-snapshot guard and trust the seeded REQUEST_CURRENT
    evidence row outright — exactly what the orchestration tests need.
    """
    from lib.beets_db import AlbumInfo
    info = AlbumInfo(
        album_id=1,
        track_count=10,
        min_bitrate_kbps=180,
        avg_bitrate_kbps=180,
        format="MP3",
        is_cbr=True,
        album_path=None,  # type: ignore[arg-type]
    )
    instance = MagicMock()
    instance.get_album_info.return_value = info
    cls = MagicMock()
    cls.return_value.__enter__ = MagicMock(return_value=instance)
    cls.return_value.__exit__ = MagicMock(return_value=False)
    return cls


def _seed_single_track(db: FakePipelineDB, request_id: int = 42) -> None:
    db.set_tracks(request_id, [{"track_number": 1, "title": "Track"}])


class TestDispatchFromDbOrchestration(unittest.TestCase):
    """Orchestration tests — assert domain state after force-import."""

    def _dispatch(self, ir=None, source_username=None,
                  source_download_log_id=None,
                  quality_gate_plan: QualityGatePlan | None = None,
                  **req_overrides):
        """Drive a force-import through the evidence-gated dispatch path.

        After U4 the importer never measures: ``dispatch_import_from_db``
        requires ``import_job_id`` and consults pre-recorded candidate +
        current quality evidence. This helper seeds matching evidence rows
        so existing orchestration tests still exercise the post-evidence
        decision logic (downgrade prevention, quality gate, denylist,
        cleanup).
        """
        from lib.dispatch import dispatch_import_from_db

        db = FakePipelineDB()
        req_kwargs = {
            "id": 42,
            "mb_release_id": "mbid-123",
            "status": "manual",
            "artist_name": "Son Ambulance",
            "album_title": "Someone Else's Deja Vu",
            "min_bitrate": 180,
            "current_spectral_bitrate": 128,
            "current_spectral_grade": "likely_transcode",
        }
        req_kwargs.update(req_overrides)
        req = make_request_row(**req_kwargs)
        db.seed_request(req)
        _seed_single_track(db, 42)
        if source_download_log_id is True:
            source_download_log_id = db.log_download(
                42,
                outcome="rejected",
                beets_distance=0.2328,
                beets_scenario="high_distance",
            )

        if ir is None:
            ir = make_import_result(decision="import", new_min_bitrate=320)
        tmpdir = tempfile.mkdtemp()
        try:
            # Realistic candidate file so snapshot_audio_files produces a
            # stable hash for the evidence row's file manifest.
            with open(os.path.join(tmpdir, "01.mp3"), "wb") as handle:
                handle.write(b"audio")

            # Enqueue an import_job — the importer-supplied ID is now
            # mandatory at the dispatch boundary.
            job = db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                payload={"failed_path": tmpdir},
            )
            import_job_id = job.id
            from lib.quality import SpectralAnalysisDetail, SpectralDetail
            preview_ir = ImportResult(spectral=SpectralDetail(
                candidate=SpectralAnalysisDetail(
                    attempted=True, grade="suspect", bitrate_kbps=160),
                existing=SpectralAnalysisDetail(
                    attempted=True, grade="genuine", bitrate_kbps=None),
            ))
            db.mark_import_job_preview_importable(
                import_job_id,
                preview_result={"import_result": msgspec.to_builtins(preview_ir)},
                message="two-sided spectral audit ready",
            )

            # Seed candidate evidence matching the on-disk snapshot.
            _seed_candidate_for_import_job(
                db, import_job_id,
                mb_release_id="mbid-candidate",
                files=snapshot_audio_files(tmpdir),
                measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=320,
                    avg_bitrate_kbps=320,
                    median_bitrate_kbps=320,
                    format="MP3",
                    spectral_grade="genuine",
                ),
                codec="mp3",
                container="mp3",
                storage_format="MP3",
            )
            # Seed current (on-disk) evidence so override-min-bitrate
            # derivation flows through the same grade-aware logic the
            # legacy branch used (compute_effective_override_bitrate of
            # min_bitrate=180 vs likely_transcode spectral=128 → 128).
            _seed_current_for_request(
                db, 42,
                mb_release_id="mbid-current",
                measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=req_kwargs.get("min_bitrate", 180),
                    avg_bitrate_kbps=req_kwargs.get("min_bitrate", 180),
                    median_bitrate_kbps=req_kwargs.get("min_bitrate", 180),
                    format="MP3",
                    spectral_bitrate_kbps=req_kwargs.get(
                        "current_spectral_bitrate"),
                    spectral_grade=req_kwargs.get("current_spectral_grade"),
                ),
                codec="mp3",
                container="mp3",
                storage_format="mp3",
            )

            mock_gate = RecordingQualityGate(result=quality_gate_plan)
            with patch_dispatch_externals() as ext, \
                 patch("lib.dispatch.subprocess_runner.parse_import_result", return_value=ir), \
                 patch("lib.beets_db.BeetsDB", _mock_beets_db_for_dispatch()), \
                 patch("lib.config.read_runtime_config",
                       return_value=CratediggerConfig(
                           beets_harness_path="/nix/store/fake/harness/run_beets_harness.sh",
                           pipeline_db_enabled=True,
                       )):
                result = dispatch_import_from_db(
                    db, request_id=42, failed_path=tmpdir,  # type: ignore[arg-type]
                    source_username=source_username,
                    import_job_id=import_job_id,
                    download_log_id=source_download_log_id,
                    quality_gate_fn=mock_gate,
                )
                cmd = ext.run.call_args[0][0] if ext.run.call_args else []
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)

        return {
            "result": result,
            "cmd": cmd,
            "db": db,
            "path": tmpdir,
            "mock_gate": mock_gate,
            "mock_jellyfin": ext.jellyfin,
            "mock_cleanup": ext.cleanup,
        }

    # --- Success path ---

    def test_successful_force_import_preserves_stop_without_terminal_acceptance(self):
        r = self._dispatch()
        self.assertTrue(r["result"].success)
        self.assertEqual(r["db"].request(42)["status"], "manual")

    def test_terminally_accepted_force_import_marks_imported(self):
        r = self._dispatch(quality_gate_plan=QualityGatePlan(
            transition=transitions.RequestTransition.to_imported(
                from_status="imported",
            ),
            successful_terminal_acceptance=True,
        ))
        self.assertTrue(r["result"].success)
        self.assertEqual(r["db"].request(42)["status"], "imported")

    def test_success_logs_with_force_import_outcome(self):
        r = self._dispatch()
        logs = r["db"].download_logs
        self.assertEqual(len(logs), 1)
        self.assertEqual(logs[0].outcome, "force_import")

    def test_successful_force_import_records_explicit_source_log(self):
        r = self._dispatch(source_download_log_id=True)
        logs = r["db"].download_logs
        self.assertEqual(logs[-1].source_download_log_id, logs[0].id)

    def test_successful_force_import_runs_post_import_pipeline(self):
        # Decision 19: force-import overrides the beets distance and nothing
        # else — it runs the identical post-import gate.
        r = self._dispatch()
        r["mock_gate"].assert_called_once()
        r["mock_jellyfin"].assert_called_once()

    def test_no_double_download_log(self):
        r = self._dispatch()
        logs = [l for l in r["db"].download_logs if l.request_id == 42]
        self.assertEqual(len(logs), 1)

    # --- Downgrade prevention ---

    def test_downgrade_prevented(self):
        ir = make_import_result(decision="downgrade",
                                new_min_bitrate=128, prev_min_bitrate=180)
        r = self._dispatch(ir=ir)
        self.assertFalse(r["result"].success)
        logged = ImportResult.from_json(r["db"].download_logs[-1].import_result)
        assert logged.spectral.candidate is not None
        assert logged.spectral.existing is not None
        self.assertEqual(logged.spectral.candidate.grade, "suspect")
        self.assertEqual(logged.spectral.existing.grade, "genuine")

    def test_downgrade_denylists_source_user(self):
        ir = make_import_result(decision="downgrade",
                                new_min_bitrate=128, prev_min_bitrate=180)
        r = self._dispatch(ir=ir, source_username="baduser")
        denylisted = [e.username for e in r["db"].denylist]
        self.assertIn("baduser", denylisted)
        self.assertEqual(r["db"].denylist[0].reason, "quality downgrade prevented")

    def test_failure_does_not_requeue(self):
        """Failed force-import must NOT requeue to wanted."""
        ir = make_import_result(decision="downgrade",
                                new_min_bitrate=128, prev_min_bitrate=180)
        r = self._dispatch(ir=ir)
        self.assertEqual(r["db"].request(42)["status"], "manual")

    def test_transcode_downgrade_does_not_requeue(self):
        ir = make_import_result(decision="transcode_downgrade",
                                new_min_bitrate=190, prev_min_bitrate=320)
        r = self._dispatch(ir=ir)
        self.assertEqual(r["db"].request(42)["status"], "manual")

    # --- Audit trail ---

    def test_failure_logs_validation_result_and_staged_path(self):
        ir = make_import_result(decision="downgrade",
                                new_min_bitrate=128, prev_min_bitrate=180)
        r = self._dispatch(ir=ir)
        log = r["db"].download_logs[0]
        self.assertEqual(log.staged_path, r["path"])
        self.assertIsNotNone(log.validation_result)
        self.assertIn("quality_downgrade", log.validation_result or "")

    # --- Seam: override bitrate derivation ---

    def test_uses_effective_override_bitrate(self):
        """Must use min(min_bitrate=180, spectral_bitrate=128) → 128."""
        r = self._dispatch()
        cmd = r["cmd"]
        idx = cmd.index("--override-min-bitrate")
        self.assertEqual(int(cmd[idx + 1]), 128)

    # --- Seam: force flag ---

    def test_force_flag_passed(self):
        r = self._dispatch()
        self.assertIn("--force", r["cmd"])

    def test_force_import_command_has_no_preview_import_result_channel(self):
        r = self._dispatch(
            min_bitrate=116,
            current_spectral_grade="likely_transcode",
            current_lossless_source_v0_probe_avg_bitrate=260,
        )

        self.assertNotIn(
            "--preview-import-result-file",
            r["cmd"],
            "Force import may bypass distance only; a stale preview "
            "ImportResult must not be passed to import_one as decision authority.",
        )

    def test_force_import_with_valid_candidate_evidence_skips_preimport_measurement(self):
        from lib.dispatch import dispatch_import_from_db

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            mb_release_id="mbid-123",
            status="manual",
            artist_name="Son Ambulance",
            album_title="Someone Else's Deja Vu",
        ))
        _seed_single_track(db)
        download_log_id = db.log_download(
            42,
            outcome="rejected",
            validation_result={"failed_path": ""},
        )
        ir = make_import_result(decision="import", new_min_bitrate=245)
        tmpdir = tempfile.mkdtemp()
        try:
            with open(os.path.join(tmpdir, "01.mp3"), "wb") as handle:
                handle.write(b"audio")
            files = snapshot_audio_files(tmpdir)
            _seed_candidate_for_download_log(
                db, download_log_id,
                mb_release_id="mbid-candidate",
                files=files,
                measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=245,
                    avg_bitrate_kbps=256,
                    median_bitrate_kbps=252,
                    format="MP3",
                    spectral_grade="genuine",
                ),
                codec="mp3",
                container="mp3",
                storage_format="MP3",
            )
            _seed_current_for_request(
                db, 42,
                mb_release_id="mbid-current",
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
            with patch_dispatch_externals() as ext, \
                 patch("lib.dispatch.subprocess_runner.parse_import_result", return_value=ir), \
                 patch("lib.beets_db.BeetsDB", _mock_beets_db_for_dispatch()), \
                 patch("lib.config.read_runtime_config",
                       return_value=CratediggerConfig(
                           beets_harness_path="/nix/store/fake/harness/run_beets_harness.sh",
                           pipeline_db_enabled=True,
                       )):
                result = dispatch_import_from_db(
                    db,  # type: ignore[arg-type]
                    request_id=42,
                    failed_path=tmpdir,  # type: ignore[arg-type]
                    source_username="alice",
                    download_log_id=download_log_id,
                    quality_gate_fn=noop_quality_gate,
                )

            self.assertTrue(result.success)
            cmd = ext.run.call_args[0][0]
            self.assertIn("--quality-evidence-action-file", cmd)
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_force_import_with_stale_candidate_evidence_requeues_to_preview(self):
        """U2: stale candidate evidence requeues the import_job for preview
        rather than hard-failing."""
        from lib.dispatch import (
            DISPATCH_CODE_REQUEUED_FOR_PREVIEW,
            dispatch_import_from_db,
        )

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            mb_release_id="mbid-123",
            status="manual",
            artist_name="Son Ambulance",
            album_title="Someone Else's Deja Vu",
        ))
        _seed_single_track(db)
        tmpdir = tempfile.mkdtemp()
        try:
            track = os.path.join(tmpdir, "01.mp3")
            with open(track, "wb") as handle:
                handle.write(b"audio")
            job = db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                payload={"failed_path": tmpdir},
                dedupe_key="manual:requeue-stale",
            )
            db.mark_import_job_preview_importable(
                job.id,
                preview_result={"verdict": "would_import"},
                message="ready",
            )
            claimed = db.claim_next_import_job(worker_id="importer")
            assert claimed is not None
            _seed_candidate_for_import_job(
                db, job.id,
                mb_release_id="mbid-123",
                files=snapshot_audio_files(tmpdir),
                measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=245,
                    avg_bitrate_kbps=256,
                    median_bitrate_kbps=252,
                    format="MP3",
                    spectral_grade="genuine",
                ),
                codec="mp3",
                container="mp3",
                storage_format="MP3",
            )
            # Mutate file so the snapshot now mismatches.
            with open(track, "ab") as handle:
                handle.write(b" changed")

            with patch_dispatch_externals() as ext, \
                 patch("lib.config.read_runtime_config",
                       return_value=CratediggerConfig(
                           beets_harness_path="/nix/store/fake/harness/run_beets_harness.sh",
                           pipeline_db_enabled=True,
                       )):
                result = dispatch_import_from_db(
                    db,  # type: ignore[arg-type]
                    request_id=42,
                    failed_path=tmpdir,  # type: ignore[arg-type]
                    source_username="alice",
                    import_job_id=job.id,
                )

            self.assertFalse(result.success)
            self.assertEqual(result.code, DISPATCH_CODE_REQUEUED_FOR_PREVIEW)
            ext.run.assert_not_called()
            # Job is back on the preview lane.
            row = next(r for r in db._import_jobs if r["id"] == job.id)
            self.assertEqual(row["status"], "queued")
            self.assertEqual(row["preview_status"], "waiting")
            self.assertIsNone(row["worker_id"])
            # Top-level message records the requeue reason (provenance
            # text from ensure_candidate_evidence_for_action).
            self.assertTrue(row.get("message"))
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_force_import_with_missing_candidate_evidence_requeues_to_preview(self):
        """U2: missing candidate evidence requeues to preview instead of failing."""
        from lib.dispatch import (
            DISPATCH_CODE_REQUEUED_FOR_PREVIEW,
            dispatch_import_from_db,
        )

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            mb_release_id="mbid-123",
            status="manual",
            artist_name="Son Ambulance",
            album_title="Someone Else's Deja Vu",
        ))
        _seed_single_track(db)
        tmpdir = tempfile.mkdtemp()
        try:
            with open(os.path.join(tmpdir, "01.mp3"), "wb") as handle:
                handle.write(b"audio")
            job = db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                payload={"failed_path": tmpdir},
                dedupe_key="manual:requeue-missing",
            )
            db.mark_import_job_preview_importable(
                job.id,
                preview_result={"verdict": "would_import"},
                message="ready",
            )
            claimed = db.claim_next_import_job(worker_id="importer")
            assert claimed is not None
            # No upsert_album_quality_evidence — candidate evidence is missing.

            with patch_dispatch_externals() as ext, \
                 patch("lib.config.read_runtime_config",
                       return_value=CratediggerConfig(
                           beets_harness_path="/nix/store/fake/harness/run_beets_harness.sh",
                           pipeline_db_enabled=True,
                       )):
                result = dispatch_import_from_db(
                    db,  # type: ignore[arg-type]
                    request_id=42,
                    failed_path=tmpdir,  # type: ignore[arg-type]
                    source_username="alice",
                    import_job_id=job.id,
                )

            self.assertFalse(result.success)
            self.assertEqual(result.code, DISPATCH_CODE_REQUEUED_FOR_PREVIEW)
            ext.run.assert_not_called()
            row = next(r for r in db._import_jobs if r["id"] == job.id)
            self.assertEqual(row["status"], "queued")
            self.assertEqual(row["preview_status"], "waiting")
            self.assertIsNone(row["worker_id"])
            self.assertTrue(row.get("message"))
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_force_import_requeue_failure_leaves_job_running(self):
        """U2: if the requeue UPDATE itself raises, dispatch returns
        DISPATCH_CODE_REQUEUE_FAILED so the importer keeps the row in
        `running` for startup recovery."""
        from lib.dispatch import (
            DISPATCH_CODE_REQUEUE_FAILED,
            dispatch_import_from_db,
        )

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            mb_release_id="mbid-123",
            status="manual",
            artist_name="Son Ambulance",
            album_title="Someone Else's Deja Vu",
        ))
        _seed_single_track(db)
        tmpdir = tempfile.mkdtemp()
        try:
            with open(os.path.join(tmpdir, "01.mp3"), "wb") as handle:
                handle.write(b"audio")
            job = db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                payload={"failed_path": tmpdir},
                dedupe_key="manual:requeue-fail",
            )
            db.mark_import_job_preview_importable(
                job.id,
                preview_result={"verdict": "would_import"},
                message="ready",
            )
            claimed = db.claim_next_import_job(worker_id="importer")
            assert claimed is not None

            from typing import cast as _cast, Any as _Any
            from unittest.mock import patch as _patch
            db_any = _cast(_Any, db)

            with patch_dispatch_externals() as ext, \
                 _patch.object(db_any, "requeue_import_job_for_preview",
                              side_effect=RuntimeError("boom")), \
                 patch("lib.config.read_runtime_config",
                       return_value=CratediggerConfig(
                           beets_harness_path="/nix/store/fake/harness/run_beets_harness.sh",
                           pipeline_db_enabled=True,
                       )):
                result = dispatch_import_from_db(
                    db,  # type: ignore[arg-type]
                    request_id=42,
                    failed_path=tmpdir,  # type: ignore[arg-type]
                    source_username="alice",
                    import_job_id=job.id,
                )

            self.assertFalse(result.success)
            self.assertEqual(result.code, DISPATCH_CODE_REQUEUE_FAILED)
            ext.run.assert_not_called()
            row = next(r for r in db._import_jobs if r["id"] == job.id)
            # Dispatch returned the failure code without flipping the row;
            # the importer is responsible for marking the row failed based
            # on the outcome code (see test_force_import_requeue_failed_marks_job_failed
            # in tests/test_import_queue.py).
            self.assertEqual(row["status"], "running")
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_force_import_requeue_zero_rows_returns_failed_outcome(self):
        """When db.requeue_import_job_for_preview returns None (the UPDATE
        matched zero rows because the job is no longer in 'running' — a
        concurrent worker requeued it or it's terminal), dispatch must
        NOT report success. Conflating the no-op with a successful
        requeue would silently hide that the job state is now indeterminate.
        Dispatch returns DISPATCH_CODE_REQUEUE_FAILED so the importer
        marks the job failed.
        """
        from lib.dispatch import (
            DISPATCH_CODE_REQUEUE_FAILED,
            dispatch_import_from_db,
        )

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            mb_release_id="mbid-zero",
            status="manual",
            artist_name="The Bug Tester",
            album_title="Zero Rows Affected",
        ))
        _seed_single_track(db)
        tmpdir = tempfile.mkdtemp()
        try:
            with open(os.path.join(tmpdir, "01.mp3"), "wb") as handle:
                handle.write(b"audio")
            job = db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                payload={"failed_path": tmpdir},
                dedupe_key="manual:zero-rows",
            )

            with patch.object(
                db, "requeue_import_job_for_preview", return_value=None,
            ), patch_dispatch_externals() as ext, patch(
                "lib.dispatch.entry_points.ensure_candidate_evidence_for_action",
                return_value=CandidateEvidenceActionResult(
                    evidence=None,
                    provenance=ActionEvidenceProvenance(
                        candidate_status="missing",
                        fallback_reason="row missing",
                        fail_closed=True,
                    ),
                ),
            ):
                result = dispatch_import_from_db(
                    db,  # type: ignore[arg-type]
                    request_id=42,
                    failed_path=tmpdir,  # type: ignore[arg-type]
                    source_username="alice",
                    import_job_id=job.id,
                )

            self.assertFalse(result.success)
            self.assertEqual(result.code, DISPATCH_CODE_REQUEUE_FAILED)
            self.assertIn("zero rows", result.message)
            ext.run.assert_not_called()
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)

    # --- Seam: preserve-source flag (issue #111) ---

    def test_preserve_source_flag_passed_on_force(self):
        """Force-import must preserve user's source FLACs until the quality
        decision — downgrade/transcode_downgrade verdicts must NOT destroy
        originals in failed_imports/."""
        r = self._dispatch()
        self.assertIn("--preserve-source", r["cmd"])

    # --- Typed result ---

    def test_returns_typed_result(self):
        r = self._dispatch()
        self.assertTrue(hasattr(r["result"], "success"))
        self.assertTrue(hasattr(r["result"], "message"))

    # --- Issue #89: force-import rejections must NOT delete source files ---
    #
    # Auto-import passes a disposable /Incoming staging directory — cleanup
    # on `downgrade` / `transcode_downgrade` is correct. Force-import passes
    # the user's `failed_imports/…` directory, which IS the only copy of
    # the source material. A cleanup there would delete the user's data
    # when the harness decides against importing.

    def test_force_downgrade_does_not_delete_source(self):
        """Issue #89: downgrade decision on force-import must not rmtree
        the failed_imports source directory."""
        ir = make_import_result(decision="downgrade",
                                new_min_bitrate=128, prev_min_bitrate=180)
        r = self._dispatch(ir=ir)
        r["mock_cleanup"].assert_not_called()

    def test_force_transcode_downgrade_does_not_delete_source(self):
        """Issue #89: transcode_downgrade on force-import must not rmtree."""
        ir = make_import_result(decision="transcode_downgrade",
                                new_min_bitrate=190, prev_min_bitrate=320)
        r = self._dispatch(ir=ir)
        r["mock_cleanup"].assert_not_called()

    def test_force_import_success_cleans_empty_source(self):
        """Issue #89 (Codex round 1): on successful force-import, beets
        has moved the files out so the source folder is empty. We MUST
        clean it — otherwise ``get_wrong_matches()`` keeps treating the
        still-existing path as an active pending entry, the
        wrong-matches tab shows a ghost row, and the album can be
        re-force-imported even though beets already has it. Cleanup on
        mark_done=True is what makes the wrong-matches tab honest.
        """
        r = self._dispatch()  # default decision="import"
        self.assertTrue(r["result"].success)
        r["mock_cleanup"].assert_called_once_with(r["path"])

class TestDispatchFromDbAdvisoryLock(unittest.TestCase):
    """Issue #92: concurrent force-import on the same request_id
    must not write duplicate download_log rows. dispatch_import_from_db
    takes a per-request advisory lock; if another session holds it, the
    call fast-fails without running any gates, subprocesses, or log writes.
    """

    def _seed_db(self) -> "FakePipelineDB":
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42, mb_release_id="mbid-123", status="manual",
            artist_name="Son Ambulance", album_title="Someone Else's Deja Vu",
        ))
        _seed_single_track(db)
        return db

    def _dispatch(self, db: "FakePipelineDB"):
        from lib.dispatch import dispatch_import_from_db
        ir = make_import_result(decision="import", new_min_bitrate=320)
        tmpdir = tempfile.mkdtemp()
        try:
            job = db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                payload={"failed_path": tmpdir},
            )
            with patch_dispatch_externals() as ext, \
                 patch("lib.dispatch.subprocess_runner.parse_import_result", return_value=ir), \
                 patch("lib.config.read_runtime_config",
                       return_value=CratediggerConfig(
                           beets_harness_path="/nix/store/fake/harness/run_beets_harness.sh",
                           pipeline_db_enabled=True,
                       )):
                result = dispatch_import_from_db(
                    db, request_id=42, failed_path=tmpdir,  # type: ignore[arg-type]
                    import_job_id=job.id,
                    quality_gate_fn=noop_quality_gate,
                )
                return result, ext, tmpdir
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_lock_acquired_with_request_id_key(self):
        """Happy path: advisory_lock is called with the import namespace + request_id."""
        from lib.pipeline_db import ADVISORY_LOCK_NAMESPACE_IMPORT
        db = self._seed_db()
        self._dispatch(db)
        self.assertIn((ADVISORY_LOCK_NAMESPACE_IMPORT, 42), db.advisory_lock_calls)

    def test_contention_fast_fails_without_side_effects(self):
        """When the advisory lock is not acquired: no subprocess, no log, no status change."""
        db = self._seed_db()
        db.set_advisory_lock_result(False)
        result, ext, _ = self._dispatch(db)

        self.assertFalse(result.success)
        self.assertIn("already in progress", result.message.lower())
        # No import_one.py subprocess
        ext.run.assert_not_called()
        # No download_log rows
        self.assertEqual(db.download_logs, [])
        # Status unchanged
        self.assertEqual(db.request(42)["status"], "manual")
        # No denylist / cooldown / attempt recording
        self.assertEqual(db.denylist, [])
        self.assertEqual(db.recorded_attempts, [])

    def test_contention_skips_evidence_lookup(self):
        """Contended call must not consult candidate evidence either.

        After U4 the importer never measures; the only evidence-side cost
        before the lock is the early bail-out, so the contended path must
        not even call ``ensure_candidate_evidence_for_action``.
        """
        db = self._seed_db()
        db.set_advisory_lock_result(False)
        tmpdir = tempfile.mkdtemp()
        try:
            from lib.dispatch import dispatch_import_from_db
            job = db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                payload={"failed_path": tmpdir},
            )
            with patch(
                "lib.dispatch.entry_points.ensure_candidate_evidence_for_action"
            ) as mock_ensure, \
                 patch("lib.config.read_runtime_config",
                       return_value=CratediggerConfig(
                           beets_harness_path="/nix/store/fake/harness/run_beets_harness.sh",
                           pipeline_db_enabled=True,
                       )):
                result = dispatch_import_from_db(
                    db, request_id=42, failed_path=tmpdir,  # type: ignore[arg-type]
                    import_job_id=job.id,
                )
            self.assertFalse(result.success)
            mock_ensure.assert_not_called()
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)


class TestDispatchFromDbRuntimeConfigSeam(unittest.TestCase):
    def test_dispatch_import_from_db_uses_shared_runtime_config_reader(self):
        from lib.dispatch import dispatch_import_from_db

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            mb_release_id="mbid-123",
            status="manual",
            artist_name="Artist",
            album_title="Album",
        ))
        _seed_single_track(db)

        cfg = CratediggerConfig(
            beets_harness_path="/nix/store/fake/harness/run_beets_harness.sh",
            pipeline_db_enabled=True,
        )
        ir = make_import_result(decision="import", new_min_bitrate=320)
        tmpdir = tempfile.mkdtemp()
        try:
            with open(os.path.join(tmpdir, "01.mp3"), "wb") as handle:
                handle.write(b"audio")
            job = db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                payload={"failed_path": tmpdir},
            )
            _seed_candidate_for_import_job(
                db, job.id,
                mb_release_id="mbid-123",
                files=snapshot_audio_files(tmpdir),
                measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=320,
                    avg_bitrate_kbps=320,
                    median_bitrate_kbps=320,
                    format="MP3",
                    spectral_grade="genuine",
                ),
                codec="mp3",
                container="mp3",
                storage_format="MP3",
            )
            with patch_dispatch_externals(), \
                 patch("lib.dispatch.subprocess_runner.parse_import_result", return_value=ir), \
                 patch("lib.config.read_runtime_config", return_value=cfg) as mock_read:
                dispatch_import_from_db(
                    db,  # type: ignore[arg-type]
                    request_id=42,
                    failed_path=tmpdir,
                    import_job_id=job.id,
                    quality_gate_fn=noop_quality_gate,
                )
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)

        # Dispatch consumes the shared reader (not a bespoke parser). Since
        # tier-2 U5, beets_subprocess_env() also reads the runtime config
        # (BEETSDIR / [Beets] keys), so "called at least once, always
        # zero-arg" is the seam contract — not exactly-once.
        self.assertGreaterEqual(mock_read.call_count, 1)
        mock_read.assert_called_with()


class TestDispatchFromDbPrecondition(unittest.TestCase):
    """U4: calling dispatch_import_from_db with neither ``import_job_id``
    nor ``download_log_id`` is a programmer error.

    After the importer-never-measures refactor, the legacy direct-measurement
    branch that ran ``inspect_local_files`` / ``measure_preimport_state`` for
    callers that omitted both IDs has been deleted. The only production
    caller (``scripts/importer.py``) always supplies ``import_job_id``;
    a call that omits both is misuse and must surface as a typed
    ``DispatchOutcome`` error rather than silently measuring.
    """

    def test_missing_both_ids_returns_bad_request(self):
        from lib.dispatch import (
            DISPATCH_CODE_BAD_REQUEST,
            dispatch_import_from_db,
        )

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            mb_release_id="mbid-123",
            status="manual",
            artist_name="Artist",
            album_title="Album",
        ))
        tmpdir = tempfile.mkdtemp()
        try:
            with patch_dispatch_externals() as ext, \
                 patch(
                     "lib.dispatch.entry_points.ensure_candidate_evidence_for_action"
                 ) as mock_ensure, \
                 patch("lib.config.read_runtime_config",
                       return_value=CratediggerConfig(
                           beets_harness_path="/nix/store/fake/harness/run_beets_harness.sh",
                           pipeline_db_enabled=True,
                       )):
                result = dispatch_import_from_db(
                    db,  # type: ignore[arg-type]
                    request_id=42,
                    failed_path=tmpdir,
                    # NOTE: deliberately omit import_job_id and download_log_id.
                )
            self.assertFalse(result.success)
            self.assertEqual(result.code, DISPATCH_CODE_BAD_REQUEST)
            self.assertIn("import_job_id", result.message)
            # No measurement helper consulted, no import_one subprocess.
            mock_ensure.assert_not_called()
            ext.run.assert_not_called()
            # No download_log row written; the request status is untouched.
            self.assertEqual(db.download_logs, [])
            self.assertEqual(db.request(42)["status"], "manual")
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)


class TestLoadEvidenceImportGateDelegation(unittest.TestCase):
    """U2: ``_load_evidence_import_gate`` delegates current-evidence loading."""

    def _candidate_result(self):
        evidence = make_album_quality_evidence(
            mb_release_id="mbid-candidate",
        )
        provenance = ActionEvidenceProvenance(
            candidate_status="reused",
            snapshot_guard="matched",
        )
        return CandidateEvidenceActionResult(
            evidence=evidence, provenance=provenance
        )

    def test_helper_returns_none_marks_current_missing(self):
        """Beets has no album → current_status='missing', not fail-closed."""
        from lib.dispatch import _load_evidence_import_gate

        db = FakePipelineDB()
        candidate_result = self._candidate_result()
        mock_helper = MagicMock(return_value=None)
        gate = _load_evidence_import_gate(
            db,  # type: ignore[arg-type]
            request_id=42,
            mb_release_id="mbid-123",
            path="/tmp/stage",
            quality_ranks=None,
            candidate_import_job_id=7,
            candidate_download_log_id=None,
            prevalidated_candidate_result=candidate_result,
            current_evidence_loader=mock_helper,
        )

        mock_helper.assert_called_once()
        self.assertIsNone(gate.current)
        self.assertEqual(gate.current_status, "missing")
        self.assertFalse(gate.current_fail_closed)
        self.assertEqual(gate.current_reason, "album not in beets")
        self.assertEqual(gate.snapshot_guard, "matched")
        self.assertIs(gate.candidate, candidate_result.evidence)

    def test_helper_fail_closed_propagates_to_gate(self):
        """Helper fail-closed result → gate current_fail_closed=True."""
        from lib.import_evidence import CurrentEvidenceActionResult
        from lib.dispatch import _load_evidence_import_gate

        db = FakePipelineDB()
        candidate_result = self._candidate_result()
        fail_provenance = ActionEvidenceProvenance(
            current_status="failed",
            snapshot_guard="not_checked",
            fallback_reason="RuntimeError: boom",
            fail_closed=True,
        )
        fail_result = CurrentEvidenceActionResult(
            evidence=None, provenance=fail_provenance
        )
        gate = _load_evidence_import_gate(
            db,  # type: ignore[arg-type]
            request_id=42,
            mb_release_id="mbid-123",
            path="/tmp/stage",
            quality_ranks=None,
            candidate_import_job_id=7,
            candidate_download_log_id=None,
            prevalidated_candidate_result=candidate_result,
            current_evidence_loader=MagicMock(return_value=fail_result),
        )

        self.assertIsNone(gate.current)
        self.assertEqual(gate.current_status, "failed")
        self.assertTrue(gate.current_fail_closed)
        self.assertEqual(gate.current_reason, "RuntimeError: boom")
        # snapshot_guard always sourced from the candidate side.
        self.assertEqual(gate.snapshot_guard, "matched")
        self.assertIs(gate.candidate, candidate_result.evidence)


if __name__ == "__main__":
    unittest.main()
