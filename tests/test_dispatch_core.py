"""Tests for dispatch_import_core — orchestration outcomes with FakePipelineDB.

Orchestration tests assert domain state: request status, download_log rows,
denylist entries, requeue behavior. Seam tests (argv, flag forwarding) are
in a separate class and explicitly labeled.
"""

import tempfile
import unittest
from unittest.mock import MagicMock, patch

import msgspec

from lib.beets_db import AlbumInfo
from lib.config import CratediggerConfig
from lib.import_queue import IMPORT_JOB_AUTOMATION, IMPORT_JOB_FORCE
from lib.dispatch.types import EvidenceImportGate
from lib.pipeline_db import DownloadLogOutcome
from lib.terminal_outcomes import ImportJobTerminal
from lib.quality import (
    AlbumQualityV0Metric,
    AudioQualityMeasurement,
    DownloadInfo,
    ImportResult,
    QualityEvidenceActionPayload,
    VerifiedLosslessProof,
)
from tests.fakes import FakeBeetsDB, FakePipelineDB
from tests.helpers import (
    make_album_quality_evidence,
    make_import_result,
    make_request_row,
    noop_quality_gate,
    patch_dispatch_externals,
)
from lib.quality_evidence import snapshot_audio_files


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


_HARNESS = "/nix/store/fake/harness/run_beets_harness.sh"


def _patch_beets_album(album_path: str | None, *, min_bitrate: int = 128):
    beets = FakeBeetsDB()
    beets._album_info_default = (
        AlbumInfo(
            album_id=1,
            track_count=1,
            min_bitrate_kbps=min_bitrate,
            avg_bitrate_kbps=min_bitrate,
            median_bitrate_kbps=min_bitrate,
            is_cbr=False,
            album_path=album_path,
            format="MP3",
        )
        if album_path is not None
        else None
    )
    return patch("lib.beets_db.BeetsDB", return_value=beets)


class TestDispatchCoreOrchestration(unittest.TestCase):
    """Orchestration tests — assert domain state via FakePipelineDB."""

    def _dispatch(self, ir=None, force=False,
                  outcome_label: DownloadLogOutcome = "success",
                  requeue_on_failure=True, override_min_bitrate=None,
                  source_username=None, target_format=None,
                  verified_lossless_target="",
                  request_overrides=None):
        from lib.dispatch import dispatch_import_core
        if ir is None:
            ir = make_import_result(decision="import", new_min_bitrate=245)

        cfg = CratediggerConfig(
            beets_harness_path=_HARNESS,
            pipeline_db_enabled=True,
            verified_lossless_target=verified_lossless_target,
        )
        dl_info = DownloadInfo(username=source_username)

        tmpdir = tempfile.mkdtemp()
        try:
            db = FakePipelineDB()
            req = make_request_row(
                id=42, status="downloading", mb_release_id="mbid-123",
                min_bitrate=180, current_spectral_bitrate=128,
                active_download_state={
                    "files": [],
                    "filetype": "flac",
                    "current_path": tmpdir,
                },
                **(request_overrides or {}),
            )
            db.seed_request(req)
            job = db.enqueue_import_job(
                IMPORT_JOB_FORCE if force else IMPORT_JOB_AUTOMATION,
                request_id=42,
                payload={"failed_path": tmpdir} if force else {},
            )
            candidate = _seed_candidate_for_import_job(
                db,
                job.id,
                mb_release_id="mbid-123",
                source_path=tmpdir,
            )
            db.mark_import_job_preview_importable(
                job.id,
                preview_result={"ready": True},
            )
            claimed = db.claim_next_import_job(worker_id="dispatch-core-test")
            assert claimed is not None
            with patch_dispatch_externals() as ext, \
                 patch("lib.dispatch.subprocess_runner.parse_import_result", return_value=ir):
                result = dispatch_import_core(
                    path=tmpdir,
                    mb_release_id="mbid-123",
                    request_id=42,
                    label="Test Artist - Test Album",
                    force=force,
                    override_min_bitrate=override_min_bitrate,
                    target_format=target_format,
                    verified_lossless_target=verified_lossless_target,
                    beets_harness_path=cfg.beets_harness_path,
                    db=db,  # type: ignore[arg-type]
                    dl_info=dl_info,
                    distance=0.05,
                    scenario="strong_match",
                    files=[MagicMock(username=source_username or "user1",
                                     filename="01 - Track.mp3")],
                    cfg=cfg,
                    outcome_label=outcome_label,
                    requeue_on_failure=requeue_on_failure,
                    candidate_import_job_id=job.id,
                    quality_gate_fn=noop_quality_gate,
                    evidence_gate_fn=lambda *_args, **_kwargs: EvidenceImportGate(
                        candidate=candidate,
                    ),
                )
                if result.terminal_outcome is not None:
                    db.persist_import_terminal_outcome(
                        result.terminal_outcome.with_job(ImportJobTerminal(
                            status="completed" if result.success else "failed",
                            result={"success": result.success},
                            message=result.message,
                            error=None if result.success else result.message,
                        ))
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
            "cleanup_calls": ext.cleanup.call_count,
            "orphan_cleanup_calls": ext.orphans.call_count,
        }

    # --- Success path ---

    def test_successful_import_marks_imported(self):
        r = self._dispatch()
        self.assertTrue(r["result"].success)
        self.assertEqual(r["db"].request(42)["status"], "imported")

    def test_successful_import_creates_one_log_row(self):
        r = self._dispatch()
        self.assertEqual(len(r["db"].download_logs), 1)
        self.assertEqual(r["db"].download_logs[0].outcome, "success")

    def test_job_owned_destructive_cleanup_is_returned_for_post_commit(self):
        r = self._dispatch()

        self.assertEqual(r["cleanup_calls"], 0)
        self.assertEqual(r["orphan_cleanup_calls"], 0)
        cleanup = r["result"].post_commit_cleanup
        assert cleanup is not None
        self.assertEqual(cleanup.staged_path, r["path"])

    def test_stale_request_stops_before_import_subprocess(self):
        from lib.dispatch import dispatch_import_core

        class StaleDB(FakePipelineDB):
            def mark_import_subprocess_started(
                self,
                request_id: int,
                timestamp: str,
            ) -> bool:
                return False

        db = StaleDB()
        db.seed_request(make_request_row(
            id=42,
            status="downloading",
            mb_release_id="mbid-123",
            active_download_state={"files": [], "filetype": "flac"},
        ))
        cfg = CratediggerConfig(
            beets_harness_path=_HARNESS,
            pipeline_db_enabled=True,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            db.seed_request(make_request_row(
                id=42,
                status="downloading",
                mb_release_id="mbid-123",
                active_download_state={
                    "files": [],
                    "filetype": "flac",
                    "current_path": tmpdir,
                },
            ))
            job = db.enqueue_import_job(
                IMPORT_JOB_AUTOMATION,
                request_id=42,
                payload={},
            )
            candidate = _seed_candidate_for_import_job(
                db,
                job.id,
                mb_release_id="mbid-123",
                source_path=tmpdir,
            )
            db.mark_import_job_preview_importable(
                job.id,
                preview_result={"ready": True},
            )
            assert db.claim_next_import_job(worker_id="stale-test") is not None
            with patch_dispatch_externals() as ext:
                outcome = dispatch_import_core(
                    path=tmpdir,
                    mb_release_id="mbid-123",
                    request_id=42,
                    label="Test Artist - Test Album",
                    force=False,
                    override_min_bitrate=None,
                    target_format=None,
                    verified_lossless_target="",
                    beets_harness_path=cfg.beets_harness_path,
                    db=db,  # type: ignore[arg-type]
                    dl_info=DownloadInfo(username="user1"),
                    distance=0.05,
                    scenario="strong_match",
                    files=[MagicMock(username="user1", filename="01.mp3")],
                    cfg=cfg,
                    candidate_import_job_id=job.id,
                    quality_gate_fn=noop_quality_gate,
                    evidence_gate_fn=lambda *_args, **_kwargs: EvidenceImportGate(
                        candidate=candidate,
                    ),
                )

        self.assertFalse(outcome.success)
        self.assertTrue(outcome.deferred)
        self.assertEqual(
            outcome.message,
            "Request state changed before import launch",
        )
        ext.run.assert_not_called()
        self.assertEqual(db.request(42)["status"], "downloading")
        self.assertEqual(db.download_logs, [])

    def test_force_job_status_change_after_enqueue_stops_before_beets(self):
        from lib.dispatch import dispatch_import_core

        db = FakePipelineDB()
        cfg = CratediggerConfig(
            beets_harness_path=_HARNESS,
            pipeline_db_enabled=True,
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            db.seed_request(make_request_row(
                id=42,
                status="wanted",
                mb_release_id="mbid-123",
            ))
            job = db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                payload={"failed_path": tmpdir},
            )
            candidate = _seed_candidate_for_import_job(
                db,
                job.id,
                mb_release_id="mbid-123",
                source_path=tmpdir,
            )
            db.mark_import_job_preview_importable(
                job.id,
                preview_result={"ready": True},
            )
            claimed = db.claim_next_import_job(worker_id="stale-force-test")
            assert claimed is not None
            self.assertEqual(claimed.expected_request_status, "wanted")

            # The job was prepared under wanted. A later request transition
            # cannot become its own expectation at the launch boundary.
            db.request(42)["status"] = "imported"
            recorder = MagicMock()
            outcome = dispatch_import_core(
                path=tmpdir,
                mb_release_id="mbid-123",
                request_id=42,
                label="Test Artist - Test Album",
                force=True,
                beets_harness_path=cfg.beets_harness_path,
                db=db,  # type: ignore[arg-type]
                dl_info=DownloadInfo(username="user1"),
                distance=0.05,
                scenario="force_import",
                cfg=cfg,
                candidate_import_job_id=job.id,
                quality_gate_fn=noop_quality_gate,
                evidence_gate_fn=lambda *_args, **_kwargs: EvidenceImportGate(
                    candidate=candidate,
                ),
                run_import_fn=recorder,
            )

        self.assertFalse(outcome.success)
        self.assertEqual(outcome.code, "launch_authority_conflict")
        recorder.assert_not_called()
        current = db.get_import_job(job.id)
        assert current is not None
        self.assertIsNone(current.beets_launch_authorized_at)

    def test_outcome_label_in_download_log(self):
        r = self._dispatch(outcome_label="force_import")
        self.assertEqual(r["db"].download_logs[0].outcome, "force_import")

    def test_start_log_names_automatic_operation_not_eventual_outcome(self):
        with self.assertLogs("cratedigger", level="INFO") as captured:
            self._dispatch(force=False, outcome_label="success")
        self.assertTrue(any(
            "AUTO-IMPORT: Test Artist - Test Album" in message
            for message in captured.output
        ))

    def test_start_log_names_force_operation(self):
        with self.assertLogs("cratedigger", level="INFO") as captured:
            self._dispatch(force=True, outcome_label="force_import")
        self.assertTrue(any(
            "FORCE-IMPORT: Test Artist - Test Album" in message
            for message in captured.output
        ))

    # --- Downgrade prevention ---

    def test_downgrade_prevented(self):
        ir = make_import_result(decision="downgrade",
                                new_min_bitrate=128, prev_min_bitrate=180)
        r = self._dispatch(ir=ir)
        self.assertFalse(r["result"].success)

    def test_downgrade_logs_rejection(self):
        ir = make_import_result(decision="downgrade",
                                new_min_bitrate=128, prev_min_bitrate=180)
        r = self._dispatch(ir=ir)
        self.assertEqual(len(r["db"].download_logs), 1)
        self.assertEqual(r["db"].download_logs[0].outcome, "rejected")
        self.assertIn("quality_downgrade", r["db"].download_logs[0].beets_scenario or "")

    def test_downgrade_denylists_user(self):
        ir = make_import_result(decision="downgrade",
                                new_min_bitrate=128, prev_min_bitrate=180)
        r = self._dispatch(ir=ir, source_username="baduser")
        denylisted = [e.username for e in r["db"].denylist]
        self.assertIn("baduser", denylisted)

    def test_persisted_candidate_evidence_rejects_before_mutating_import(self):
        from lib.dispatch import dispatch_import_core

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            status="downloading",
            active_download_state={"files": [], "filetype": "flac"},
        ))
        log_id = db.log_download(request_id=42, outcome="rejected")
        job = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            payload={"failed_path": "/tmp/pending"},
        )

        tmpdir = tempfile.mkdtemp()
        current_dir = tempfile.mkdtemp()
        try:
            with open(f"{tmpdir}/01.flac", "wb") as handle:
                handle.write(b"audio")
            with open(f"{current_dir}/01.mp3", "wb") as handle:
                handle.write(b"current")
            files = snapshot_audio_files(tmpdir)
            _seed_candidate_for_download_log(
                db, log_id,
                mb_release_id="mbid-123",
                files=files,
                measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=900,
                    avg_bitrate_kbps=900,
                    median_bitrate_kbps=900,
                    format="FLAC",
                    spectral_grade="suspect",
                    spectral_bitrate_kbps=128,
                    spectral_subject="source",
                    spectral_provenance="measured",
                ),
                codec="flac",
                container="flac",
                storage_format="flac",
                target_format="opus 128",
                v0_metric=AlbumQualityV0Metric(
                    min_bitrate_kbps=141,
                    avg_bitrate_kbps=240,
                    median_bitrate_kbps=240,
                    subject="source",
                ),
            )
            _seed_current_for_request(
                db, 42,
                mb_release_id="mbid-123",
                files=snapshot_audio_files(current_dir),
                measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=116,
                    avg_bitrate_kbps=131,
                    median_bitrate_kbps=131,
                    format="Opus",
                    spectral_grade="likely_transcode",
                    spectral_bitrate_kbps=96,
                    spectral_subject="source",
                    spectral_provenance="carried",
                ),
                codec="opus",
                container="opus",
                storage_format="opus",
                v0_metric=AlbumQualityV0Metric(
                    min_bitrate_kbps=211,
                    avg_bitrate_kbps=260,
                    median_bitrate_kbps=260,
                    subject="source",
                ),
            )
            cfg = CratediggerConfig(
                beets_harness_path=_HARNESS,
                pipeline_db_enabled=True,
                verified_lossless_target="opus 128",
            )
            with patch_dispatch_externals() as ext:
                ext.run.side_effect = AssertionError(
                    "importer measurement/probe mutant executed"
                )
                with _patch_beets_album(current_dir, min_bitrate=116):
                    result = dispatch_import_core(
                        path=tmpdir,
                        mb_release_id="mbid-123",
                        request_id=42,
                        label="Test Artist - Test Album",
                        force=True,
                        target_format="opus 128",
                        verified_lossless_target="opus 128",
                        beets_harness_path=cfg.beets_harness_path,
                        db=db,  # type: ignore[arg-type]
                        dl_info=DownloadInfo(username="baduser"),
                        distance=0.99,
                        scenario="force_import",
                        files=[MagicMock(username="baduser", filename="01.flac")],
                        cfg=cfg,
                        requeue_on_failure=False,
                        candidate_download_log_id=log_id,
                        candidate_import_job_id=job.id,
                    )

            self.assertFalse(result.success)
            ext.run.assert_not_called()
            self.assertIsNotNone(result.terminal_outcome)
            assert result.terminal_outcome is not None
            self.assertEqual(result.terminal_outcome.audit.outcome, "rejected")
            self.assertEqual(
                result.terminal_outcome.audit.source_download_log_id,
                log_id,
            )
            denylisted = [
                entry.username for entry in result.terminal_outcome.denylists
            ]
            self.assertIn("baduser", denylisted)
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)
            shutil.rmtree(current_dir, ignore_errors=True)

    def test_persisted_candidate_evidence_imports_via_evidence_action_file(self):
        from lib.dispatch import dispatch_import_core

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            status="downloading",
            mb_release_id="mbid-123",
            active_download_state={"files": [], "filetype": "flac"},
        ))

        tmpdir = tempfile.mkdtemp()
        current_dir = tempfile.mkdtemp()
        try:
            with open(f"{tmpdir}/01.mp3", "wb") as handle:
                handle.write(b"audio")
            with open(f"{current_dir}/01.mp3", "wb") as handle:
                handle.write(b"current")
            job = db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                payload={"failed_path": tmpdir},
            )
            import_job_id = job.id
            files = snapshot_audio_files(tmpdir)
            _seed_candidate_for_import_job(
                db, import_job_id,
                mb_release_id="mbid-123",
                source_path=tmpdir,
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
            db.mark_import_job_preview_importable(
                job.id,
                preview_result={"ready": True},
            )
            assert db.claim_next_import_job(worker_id="dispatch-test") is not None
            _seed_current_for_request(
                db, 42,
                mb_release_id="mbid-123-current",
                files=snapshot_audio_files(current_dir),
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
            cfg = CratediggerConfig(
                beets_harness_path=_HARNESS,
                pipeline_db_enabled=True,
            )
            ir = make_import_result(decision="import", new_min_bitrate=245)
            decoded_payload: dict[str, QualityEvidenceActionPayload] = {}
            with patch_dispatch_externals() as ext, \
                 patch("lib.dispatch.subprocess_runner.parse_import_result", return_value=ir), \
                 _patch_beets_album(current_dir, min_bitrate=128):
                def run_side_effect(cmd, *_args, **_kwargs):
                    idx = cmd.index("--quality-evidence-action-file")
                    with open(cmd[idx + 1], "rb") as handle:
                        decoded_payload["payload"] = msgspec.json.decode(
                            handle.read(),
                            type=QualityEvidenceActionPayload,
                        )
                    return MagicMock(returncode=0, stdout="", stderr="")

                ext.run.side_effect = run_side_effect
                result = dispatch_import_core(
                    path=tmpdir,
                    mb_release_id="mbid-123",
                    request_id=42,
                    label="Test Artist - Test Album",
                    beets_harness_path=cfg.beets_harness_path,
                    db=db,  # type: ignore[arg-type]
                    dl_info=DownloadInfo(username="user1"),
                    distance=0.05,
                    scenario="strong_match",
                    files=[MagicMock(username="user1", filename="01.mp3")],
                    cfg=cfg,
                    candidate_import_job_id=import_job_id,
                    quality_gate_fn=noop_quality_gate,
                )

            self.assertTrue(result.success)
            cmd = ext.run.call_args[0][0]
            self.assertIn("--quality-evidence-action-file", cmd)
            self.assertNotIn("--preview-import-result-file", cmd)
            payload = decoded_payload["payload"]
            # Post-migration 021: candidate evidence is content-addressed by
            # (mb_release_id, snapshot_fingerprint); addressing back to the
            # import_job is via the FK we wired in the helper.
            self.assertEqual(payload.candidate.mb_release_id, "mbid-123")
            assert payload.current is not None
            self.assertEqual(payload.current.mb_release_id, "mbid-123-current")
            self.assertIs(payload.decision["imported"], True)
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)
            shutil.rmtree(current_dir, ignore_errors=True)

    def test_legacy_successful_lossy_import_clears_old_verified_lossless_proof(self):
        from lib.dispatch import _refresh_current_evidence_after_import

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            mb_release_id="mbid-123",
            status="unsearchable",
            verified_lossless=False,
        ))
        proof = VerifiedLosslessProof(
            provenance="measured",
            source="flac",
            classifier="spectral_verified_lossless",
            detail="genuine",
        )
        _seed_current_for_request(
            db, 42,
            mb_release_id="mbid-123",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=116,
                avg_bitrate_kbps=128,
                median_bitrate_kbps=127,
                format="Opus",
            ),
            verified_lossless_proof=proof,
            storage_format="Opus",
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            with open(f"{tmpdir}/01.mp3", "wb") as handle:
                handle.write(b"audio")
            with _patch_beets_album(tmpdir, min_bitrate=245):
                _refresh_current_evidence_after_import(
                    db,  # type: ignore[arg-type]
                    request_id=42,
                    mb_release_id="mbid-123",
                    quality_ranks=None,
                    source_candidate=None,
                    import_result=ImportResult(
                        decision="import",
                        source_measurement=AudioQualityMeasurement(
                            min_bitrate_kbps=245,
                            avg_bitrate_kbps=256,
                            median_bitrate_kbps=252,
                            format="MP3",
                        ),
                    ),
                )

        # After the refresh, request_current FK points at the new evidence.
        refreshed_id = db.get_request_current_evidence_id(42)
        self.assertIsNotNone(refreshed_id)
        loaded = db.load_album_quality_evidence_by_id(refreshed_id)
        assert loaded is not None
        self.assertIsNone(loaded.verified_lossless_proof)

    def test_persisted_candidate_evidence_imports_when_no_current_album(self):
        from lib.dispatch import dispatch_import_core

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            status="downloading",
            mb_release_id="mbid-123",
            active_download_state={"files": [], "filetype": "flac"},
        ))

        tmpdir = tempfile.mkdtemp()
        try:
            with open(f"{tmpdir}/01.mp3", "wb") as handle:
                handle.write(b"audio")
            job = db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                payload={"failed_path": tmpdir},
            )
            _seed_candidate_for_import_job(
                db, job.id,
                mb_release_id="mbid-123",
                source_path=tmpdir,
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
            db.mark_import_job_preview_importable(
                job.id,
                preview_result={"ready": True},
            )
            assert db.claim_next_import_job(worker_id="dispatch-test") is not None
            cfg = CratediggerConfig(
                beets_harness_path=_HARNESS,
                pipeline_db_enabled=True,
            )
            ir = make_import_result(decision="import", new_min_bitrate=245)
            decoded_payload: dict[str, QualityEvidenceActionPayload] = {}
            with patch_dispatch_externals() as ext, \
                 patch("lib.dispatch.subprocess_runner.parse_import_result", return_value=ir), \
                 _patch_beets_album(None):
                def run_side_effect(cmd, *_args, **_kwargs):
                    idx = cmd.index("--quality-evidence-action-file")
                    with open(cmd[idx + 1], "rb") as handle:
                        decoded_payload["payload"] = msgspec.json.decode(
                            handle.read(),
                            type=QualityEvidenceActionPayload,
                        )
                    return MagicMock(returncode=0, stdout="", stderr="")

                ext.run.side_effect = run_side_effect
                result = dispatch_import_core(
                    path=tmpdir,
                    mb_release_id="mbid-123",
                    request_id=42,
                    label="Test Artist - Test Album",
                    beets_harness_path=cfg.beets_harness_path,
                    db=db,  # type: ignore[arg-type]
                    dl_info=DownloadInfo(username="user1"),
                    distance=0.05,
                    scenario="strong_match",
                    files=[MagicMock(username="user1", filename="01.mp3")],
                    cfg=cfg,
                    candidate_import_job_id=job.id,
                    quality_gate_fn=noop_quality_gate,
                )

            self.assertTrue(result.success)
            payload = decoded_payload["payload"]
            self.assertIsNone(payload.current)
            self.assertEqual(payload.provenance.current_status, "missing")
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_stale_current_backfill_requires_fresh_enrichment_before_decision(self):
        from lib.dispatch import dispatch_import_core

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="downloading"))

        tmpdir = tempfile.mkdtemp()
        current_dir = tempfile.mkdtemp()
        try:
            with open(f"{tmpdir}/01.mp3", "wb") as handle:
                handle.write(b"candidate")
            with open(f"{current_dir}/01.mp3", "wb") as handle:
                handle.write(b"current")
            old_current_files = snapshot_audio_files(current_dir)
            with open(f"{current_dir}/01.mp3", "ab") as handle:
                handle.write(b" changed")
            job = db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                payload={"failed_path": tmpdir},
            )
            _seed_candidate_for_import_job(
                db, job.id,
                mb_release_id="mbid-123-candidate",
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
            _seed_current_for_request(
                db, 42,
                mb_release_id="mbid-123",
                files=old_current_files,
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
            cfg = CratediggerConfig(
                beets_harness_path=_HARNESS,
                pipeline_db_enabled=True,
            )
            with patch_dispatch_externals() as ext, \
                 _patch_beets_album(current_dir, min_bitrate=320):
                ext.run.side_effect = AssertionError(
                    "importer measurement/probe mutant executed"
                )
                result = dispatch_import_core(
                    path=tmpdir,
                    mb_release_id="mbid-123",
                    request_id=42,
                    label="Test Artist - Test Album",
                    beets_harness_path=cfg.beets_harness_path,
                    db=db,  # type: ignore[arg-type]
                    dl_info=DownloadInfo(username="user1"),
                    distance=0.05,
                    scenario="strong_match",
                    files=[MagicMock(username="user1", filename="01.mp3")],
                    cfg=cfg,
                    requeue_on_failure=False,
                    candidate_import_job_id=job.id,
                )

            self.assertFalse(result.success)
            self.assertIn("Installed HAVE analysis failed", result.message)
            self.assertEqual(result.code, "have_analysis_error")
            ext.run.assert_not_called()
            refreshed_id = db.get_request_current_evidence_id(42)
            self.assertIsNotNone(refreshed_id)
            refreshed = db.load_album_quality_evidence_by_id(refreshed_id)
            assert refreshed is not None
            self.assertEqual(refreshed.measurement.min_bitrate_kbps, 320)
            self.assertIsNone(refreshed.measurement.spectral_grade)
            self.assertIsNone(refreshed.v0_metric)
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)
            shutil.rmtree(current_dir, ignore_errors=True)

    def test_persisted_candidate_evidence_fails_when_current_album_has_no_files(self):
        from lib.dispatch import dispatch_import_core

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="downloading"))

        tmpdir = tempfile.mkdtemp()
        current_dir = tempfile.mkdtemp()
        try:
            with open(f"{tmpdir}/01.mp3", "wb") as handle:
                handle.write(b"candidate")
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
            cfg = CratediggerConfig(
                beets_harness_path=_HARNESS,
                pipeline_db_enabled=True,
            )
            with patch_dispatch_externals() as ext, \
                 _patch_beets_album(current_dir, min_bitrate=320):
                result = dispatch_import_core(
                    path=tmpdir,
                    mb_release_id="mbid-123",
                    request_id=42,
                    label="Test Artist - Test Album",
                    beets_harness_path=cfg.beets_harness_path,
                    db=db,  # type: ignore[arg-type]
                    dl_info=DownloadInfo(username="user1"),
                    distance=0.05,
                    scenario="strong_match",
                    files=[MagicMock(username="user1", filename="01.mp3")],
                    cfg=cfg,
                    requeue_on_failure=False,
                    candidate_import_job_id=job.id,
                )

            self.assertFalse(result.success)
            self.assertIn("Installed HAVE analysis failed", result.message)
            ext.run.assert_not_called()
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)
            shutil.rmtree(current_dir, ignore_errors=True)

    def test_persisted_candidate_evidence_fails_closed_on_current_error(self):
        from lib.dispatch import dispatch_import_core

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="downloading"))

        tmpdir = tempfile.mkdtemp()
        try:
            with open(f"{tmpdir}/01.mp3", "wb") as handle:
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
            cfg = CratediggerConfig(
                beets_harness_path=_HARNESS,
                pipeline_db_enabled=True,
            )
            with patch_dispatch_externals() as ext, \
                 _patch_beets_album(tmpdir, min_bitrate=128), \
                 patch(
                     "lib.import_evidence.ensure_current_evidence_for_action",
                     side_effect=RuntimeError("beets unavailable"),
                 ):
                result = dispatch_import_core(
                    path=tmpdir,
                    mb_release_id="mbid-123",
                    request_id=42,
                    label="Test Artist - Test Album",
                    beets_harness_path=cfg.beets_harness_path,
                    db=db,  # type: ignore[arg-type]
                    dl_info=DownloadInfo(username="user1"),
                    distance=0.05,
                    scenario="strong_match",
                    files=[MagicMock(username="user1", filename="01.mp3")],
                    cfg=cfg,
                    candidate_import_job_id=job.id,
                )

            self.assertFalse(result.success)
            self.assertIn("Installed HAVE analysis failed", result.message)
            ext.run.assert_not_called()
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_downgrade_preserves_validation_result_and_staged_path(self):
        ir = make_import_result(decision="downgrade",
                                new_min_bitrate=128, prev_min_bitrate=180)
        r = self._dispatch(ir=ir, requeue_on_failure=False)
        log = r["db"].download_logs[0]
        self.assertEqual(log.staged_path, r["path"])
        self.assertIsNotNone(log.validation_result)
        self.assertIn("quality_downgrade", log.validation_result or "")

    # --- Requeue behavior ---

    def test_failed_no_requeue_stays_downloading(self):
        """When requeue_on_failure=False, status should not change to wanted."""
        ir = make_import_result(decision="downgrade",
                                new_min_bitrate=128, prev_min_bitrate=180)
        r = self._dispatch(ir=ir, requeue_on_failure=False)
        # Should NOT have transitioned to wanted
        self.assertNotEqual(r["db"].request(42)["status"], "wanted")

    def test_failed_with_requeue_transitions_to_wanted(self):
        """When requeue_on_failure=True, failed import requeues to wanted."""
        ir = make_import_result(decision="downgrade",
                                new_min_bitrate=128, prev_min_bitrate=180)
        r = self._dispatch(ir=ir, requeue_on_failure=True)
        row = r["db"].request(42)
        self.assertEqual(row["status"], "wanted")
        self.assertEqual(row["validation_attempts"], 1)
        self.assertIsNotNone(row["last_attempt_at"])
        self.assertIsNotNone(row["next_retry_after"])

    # --- Transcode paths ---

    def test_transcode_upgrade_requeues_for_better(self):
        ir = make_import_result(decision="transcode_upgrade",
                                new_min_bitrate=227)
        r = self._dispatch(ir=ir)
        self.assertTrue(r["result"].success)
        # Should be requeued to wanted for upgrade search
        self.assertEqual(r["db"].request(42)["status"], "wanted")

    def test_transcode_upgrade_denylists_user(self):
        ir = make_import_result(decision="transcode_upgrade",
                                new_min_bitrate=227)
        r = self._dispatch(ir=ir, source_username="transuser")
        denylisted = [e.username for e in r["db"].denylist]
        self.assertIn("transuser", denylisted)

    def test_transcode_downgrade_no_requeue_when_disabled(self):
        ir = make_import_result(decision="transcode_downgrade",
                                new_min_bitrate=190, prev_min_bitrate=320)
        r = self._dispatch(ir=ir, requeue_on_failure=False)
        self.assertNotEqual(r["db"].request(42)["status"], "wanted")

    # --- lossless_source_locked + search-narrowing (R7, AE2) ---

    def test_lossless_source_locked_narrows_search_filetype_override(self):
        """R7 / AE2: when a lossy candidate hits lossless_source_locked
        during importer dispatch, the request's search_filetype_override
        narrows to 'lossless' so future cycles only ask for lossless
        candidates that can actually win against the existing
        lossless-source library row.

        Without this narrowing, the search planner keeps re-asking
        Soulseek with no filetype filter, each new peer serves the
        same lossy file, and the lock fires repeatedly. The narrowing
        closes that wasted-cycle window.
        """
        ir = make_import_result(decision="lossless_source_locked")
        r = self._dispatch(
            ir=ir,
            request_overrides={
                "search_filetype_override": "lossless,mp3 v0,mp3 320",
            },
        )
        self.assertEqual(
            r["db"].request(42)["search_filetype_override"], "lossless")

    def test_lossless_source_locked_narrowing_is_idempotent(self):
        """AE7: when the override is already 'lossless', the lock
        firing again is a no-op (no spurious DB write that would churn
        change tracking or audit logs)."""
        ir = make_import_result(decision="lossless_source_locked")
        r = self._dispatch(
            ir=ir,
            request_overrides={"search_filetype_override": "lossless"},
        )
        self.assertEqual(
            r["db"].request(42)["search_filetype_override"], "lossless")


class TestDispatchCoreSeams(unittest.TestCase):
    """Seam tests — assert subprocess argv construction."""

    def _get_cmd(self, **kwargs):
        from lib.dispatch import dispatch_import_core
        ir = kwargs.pop("ir", make_import_result())
        beets_directory = kwargs.pop("beets_directory", "")
        cfg = CratediggerConfig(
            beets_harness_path=_HARNESS,
            beets_directory=beets_directory,
            pipeline_db_enabled=True,
        )
        tmpdir = tempfile.mkdtemp()
        try:
            db = FakePipelineDB()
            db.seed_request(make_request_row(
                id=42,
                status="downloading",
                mb_release_id="mbid-123",
                active_download_state={
                    "files": [],
                    "filetype": "flac",
                    "current_path": tmpdir,
                },
            ))
            force = bool(kwargs.get("force", False))
            job = db.enqueue_import_job(
                IMPORT_JOB_FORCE if force else IMPORT_JOB_AUTOMATION,
                request_id=42,
                payload={"failed_path": tmpdir} if force else {},
            )
            candidate = _seed_candidate_for_import_job(
                db,
                job.id,
                mb_release_id="mbid-123",
                source_path=tmpdir,
            )
            db.mark_import_job_preview_importable(
                job.id,
                preview_result={"ready": True},
            )
            assert db.claim_next_import_job(worker_id="seam-test") is not None
            with patch_dispatch_externals() as ext, \
                 patch("lib.dispatch.subprocess_runner.parse_import_result", return_value=ir):
                dispatch_import_core(
                    path=tmpdir,
                    mb_release_id="mbid-123",
                    request_id=42,
                    label="Test",
                    beets_harness_path=_HARNESS,
                    db=db,  # type: ignore[arg-type]
                    dl_info=DownloadInfo(),
                    cfg=cfg,
                    candidate_import_job_id=job.id,
                    quality_gate_fn=noop_quality_gate,
                    evidence_gate_fn=lambda *_args, **_kwargs: EvidenceImportGate(
                        candidate=candidate,
                    ),
                    **kwargs,
                )
                return ext.run.call_args[0][0] if ext.run.call_args else []
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_force_flag_passed(self):
        cmd = self._get_cmd(force=True)
        self.assertIn("--force", cmd)

    def test_no_force_by_default(self):
        cmd = self._get_cmd(force=False)
        self.assertNotIn("--force", cmd)

    def test_override_min_bitrate_passed(self):
        cmd = self._get_cmd(override_min_bitrate=128)
        idx = cmd.index("--override-min-bitrate")
        self.assertEqual(cmd[idx + 1], "128")

    def test_verified_lossless_target_flag(self):
        cmd = self._get_cmd(verified_lossless_target="opus 128")
        self.assertIn("--verified-lossless-target", cmd)
        idx = cmd.index("--verified-lossless-target")
        self.assertEqual(cmd[idx + 1], "opus 128")

    def test_target_format_flag(self):
        cmd = self._get_cmd(target_format="flac")
        self.assertIn("--target-format", cmd)
        idx = cmd.index("--target-format")
        self.assertEqual(cmd[idx + 1], "flac")

    def test_shared_import_one_command_supports_preview_without_request_id(self):
        from lib.dispatch import build_import_one_command

        cmd = build_import_one_command(
            path="/tmp/album",
            mb_release_id="mbid-123",
            beets_harness_path=_HARNESS,
            dry_run=True,
            preserve_source=True,
        )

        self.assertIn("--dry-run", cmd)
        self.assertIn("--preserve-source", cmd)
        self.assertNotIn("--request-id", cmd)

    def test_shared_import_one_command_does_not_accept_preview_result_file(self):
        from lib.dispatch import build_import_one_command

        cmd = build_import_one_command(
            path="/tmp/album",
            mb_release_id="mbid-123",
            beets_harness_path=_HARNESS,
        )

        self.assertNotIn("--preview-import-result-file", cmd)

    def test_dispatch_core_has_no_preview_import_result_channel(self):
        cmd = self._get_cmd()

        self.assertNotIn("--preview-import-result-file", cmd)


if __name__ == "__main__":
    unittest.main()
