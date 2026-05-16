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
from lib.import_queue import IMPORT_JOB_MANUAL, manual_import_payload
from lib.quality import (
    AlbumQualityV0Metric,
    AudioQualityMeasurement,
    DownloadInfo,
    ImportResult,
    QualityEvidenceActionPayload,
    VerifiedLosslessProof,
)
from tests.fakes import FakePipelineDB
from tests.helpers import (
    make_album_quality_evidence,
    make_import_result,
    make_request_row,
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
    beets = MagicMock()
    beets.__enter__.return_value = beets
    beets.__exit__.return_value = None
    beets.get_album_info.return_value = (
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

    def _dispatch(self, ir=None, force=False, outcome_label="success",
                  requeue_on_failure=True, override_min_bitrate=None,
                  source_username=None, target_format=None,
                  verified_lossless_target="",
                  request_overrides=None):
        from lib.import_dispatch import dispatch_import_core
        if ir is None:
            ir = make_import_result(decision="import", new_min_bitrate=245)

        db = FakePipelineDB()
        req = make_request_row(
            id=42, status="downloading",
            min_bitrate=180, current_spectral_bitrate=128,
            **(request_overrides or {}),
        )
        db.seed_request(req)

        cfg = CratediggerConfig(
            beets_harness_path=_HARNESS,
            pipeline_db_enabled=True,
            verified_lossless_target=verified_lossless_target,
        )
        dl_info = DownloadInfo(username=source_username)

        tmpdir = tempfile.mkdtemp()
        try:
            with patch_dispatch_externals() as ext, \
                 patch("lib.import_dispatch._check_quality_gate_core"), \
                 patch("lib.import_dispatch.parse_import_result", return_value=ir):
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

    def test_outcome_label_in_download_log(self):
        r = self._dispatch(outcome_label="force_import")
        self.assertEqual(r["db"].download_logs[0].outcome, "force_import")

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
        from lib.import_dispatch import dispatch_import_core

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="downloading"))
        log_id = db.log_download(request_id=42, outcome="rejected")

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
                ),
                codec="flac",
                container="flac",
                storage_format="flac",
                target_format="opus 128",
                v0_metric=AlbumQualityV0Metric(
                    min_bitrate_kbps=141,
                    avg_bitrate_kbps=240,
                    median_bitrate_kbps=240,
                    source_lineage="lossless_source",
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
                ),
                codec="opus",
                container="opus",
                storage_format="opus",
                v0_metric=AlbumQualityV0Metric(
                    min_bitrate_kbps=211,
                    avg_bitrate_kbps=260,
                    median_bitrate_kbps=260,
                    source_lineage="lossless_source",
                ),
            )
            cfg = CratediggerConfig(
                beets_harness_path=_HARNESS,
                pipeline_db_enabled=True,
                verified_lossless_target="opus 128",
            )
            with patch_dispatch_externals() as ext:
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
                    )

            self.assertFalse(result.success)
            ext.run.assert_not_called()
            self.assertEqual(db.download_logs[-1].outcome, "rejected")
            denylisted = [entry.username for entry in db.denylist]
            self.assertIn("baduser", denylisted)
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)
            shutil.rmtree(current_dir, ignore_errors=True)

    def test_persisted_candidate_evidence_imports_via_evidence_action_file(self):
        from lib.import_dispatch import dispatch_import_core

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="downloading"))

        tmpdir = tempfile.mkdtemp()
        current_dir = tempfile.mkdtemp()
        try:
            with open(f"{tmpdir}/01.mp3", "wb") as handle:
                handle.write(b"audio")
            with open(f"{current_dir}/01.mp3", "wb") as handle:
                handle.write(b"current")
            job = db.enqueue_import_job(
                IMPORT_JOB_MANUAL,
                request_id=42,
                payload=manual_import_payload(failed_path=tmpdir),
            )
            import_job_id = job.id
            files = snapshot_audio_files(tmpdir)
            _seed_candidate_for_import_job(
                db, import_job_id,
                mb_release_id="mbid-123",
                files=files,
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
                storage_format="mp3 128",
            )
            cfg = CratediggerConfig(
                beets_harness_path=_HARNESS,
                pipeline_db_enabled=True,
            )
            ir = make_import_result(decision="import", new_min_bitrate=245)
            decoded_payload: dict[str, QualityEvidenceActionPayload] = {}
            with patch_dispatch_externals() as ext, \
                 patch("lib.import_dispatch._check_quality_gate_core"), \
                 patch("lib.import_dispatch.parse_import_result", return_value=ir), \
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
        from lib.import_dispatch import _refresh_current_evidence_after_import

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            mb_release_id="mbid-123",
            status="manual",
            verified_lossless=False,
        ))
        proof = VerifiedLosslessProof(
            proof_origin="candidate_import",
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
                verified_lossless=True,
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
                        new_measurement=AudioQualityMeasurement(
                            min_bitrate_kbps=245,
                            avg_bitrate_kbps=256,
                            median_bitrate_kbps=252,
                            format="MP3 V0",
                            verified_lossless=False,
                        ),
                    ),
                )

        # After the refresh, request_current FK points at the new evidence.
        refreshed_id = db.get_request_current_evidence_id(42)
        self.assertIsNotNone(refreshed_id)
        loaded = db.load_album_quality_evidence_by_id(refreshed_id)
        assert loaded is not None
        self.assertFalse(loaded.measurement.verified_lossless)
        self.assertIsNone(loaded.verified_lossless_proof)

    def test_persisted_candidate_evidence_imports_when_no_current_album(self):
        from lib.import_dispatch import dispatch_import_core

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="downloading"))

        tmpdir = tempfile.mkdtemp()
        try:
            with open(f"{tmpdir}/01.mp3", "wb") as handle:
                handle.write(b"audio")
            job = db.enqueue_import_job(
                IMPORT_JOB_MANUAL,
                request_id=42,
                payload=manual_import_payload(failed_path=tmpdir),
            )
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
                beets_harness_path=_HARNESS,
                pipeline_db_enabled=True,
            )
            ir = make_import_result(decision="import", new_min_bitrate=245)
            decoded_payload: dict[str, QualityEvidenceActionPayload] = {}
            with patch_dispatch_externals() as ext, \
                 patch("lib.import_dispatch._check_quality_gate_core"), \
                 patch("lib.import_dispatch.parse_import_result", return_value=ir), \
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
                )

            self.assertTrue(result.success)
            payload = decoded_payload["payload"]
            self.assertIsNone(payload.current)
            self.assertEqual(payload.provenance.current_status, "missing")
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_persisted_candidate_evidence_backfills_stale_current_before_decision(self):
        from lib.import_dispatch import dispatch_import_core

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
                IMPORT_JOB_MANUAL,
                request_id=42,
                payload=manual_import_payload(failed_path=tmpdir),
            )
            _seed_candidate_for_import_job(
                db, job.id,
                mb_release_id="mbid-123-candidate",
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
                storage_format="mp3 128",
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

            from lib.import_dispatch import DISPATCH_CODE_QUALITY_PIPELINE_REJECTED

            self.assertFalse(result.success)
            self.assertIn("Rejected by persisted quality evidence", result.message)
            self.assertEqual(result.code, DISPATCH_CODE_QUALITY_PIPELINE_REJECTED)
            ext.run.assert_not_called()
            refreshed_id = db.get_request_current_evidence_id(42)
            self.assertIsNotNone(refreshed_id)
            refreshed = db.load_album_quality_evidence_by_id(refreshed_id)
            assert refreshed is not None
            self.assertEqual(refreshed.measurement.min_bitrate_kbps, 320)
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)
            shutil.rmtree(current_dir, ignore_errors=True)

    def test_persisted_candidate_evidence_fails_when_current_album_has_no_files(self):
        from lib.import_dispatch import dispatch_import_core

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="downloading"))

        tmpdir = tempfile.mkdtemp()
        current_dir = tempfile.mkdtemp()
        try:
            with open(f"{tmpdir}/01.mp3", "wb") as handle:
                handle.write(b"candidate")
            job = db.enqueue_import_job(
                IMPORT_JOB_MANUAL,
                request_id=42,
                payload=manual_import_payload(failed_path=tmpdir),
            )
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
            self.assertIn("Current quality evidence unavailable", result.message)
            ext.run.assert_not_called()
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)
            shutil.rmtree(current_dir, ignore_errors=True)

    def test_persisted_candidate_evidence_fails_closed_on_current_error(self):
        from lib.import_dispatch import dispatch_import_core

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="downloading"))

        tmpdir = tempfile.mkdtemp()
        try:
            with open(f"{tmpdir}/01.mp3", "wb") as handle:
                handle.write(b"audio")
            job = db.enqueue_import_job(
                IMPORT_JOB_MANUAL,
                request_id=42,
                payload=manual_import_payload(failed_path=tmpdir),
            )
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
                beets_harness_path=_HARNESS,
                pipeline_db_enabled=True,
            )
            with patch_dispatch_externals() as ext, \
                 _patch_beets_album(tmpdir, min_bitrate=128), \
                 patch(
                     "lib.import_dispatch.ensure_current_evidence_for_action",
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
            self.assertIn("Current quality evidence unavailable", result.message)
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


class TestDispatchCoreSeams(unittest.TestCase):
    """Seam tests — assert subprocess argv construction."""

    def _get_cmd(self, **kwargs):
        from lib.import_dispatch import dispatch_import_core
        ir = kwargs.pop("ir", make_import_result())
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="downloading"))
        cfg = CratediggerConfig(
            beets_harness_path=_HARNESS,
            pipeline_db_enabled=True,
        )
        tmpdir = tempfile.mkdtemp()
        try:
            with patch_dispatch_externals() as ext, \
                 patch("lib.import_dispatch._check_quality_gate_core"), \
                 patch("lib.import_dispatch.parse_import_result", return_value=ir):
                dispatch_import_core(
                    path=tmpdir,
                    mb_release_id="mbid-123",
                    request_id=42,
                    label="Test",
                    beets_harness_path=_HARNESS,
                    db=db,  # type: ignore[arg-type]
                    dl_info=DownloadInfo(),
                    cfg=cfg,
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
        from lib.import_dispatch import build_import_one_command

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
        from lib.import_dispatch import build_import_one_command

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
