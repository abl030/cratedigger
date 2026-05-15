"""Tests for dispatch_import_from_db — force/manual import through the real pipeline.

Orchestration tests use FakePipelineDB to assert domain state (request status,
log rows, denylist). Seam tests verify argv/config wiring.
"""

import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from lib.config import CratediggerConfig
from lib.import_queue import IMPORT_JOB_MANUAL, manual_import_payload
from lib.quality import (
    ALBUM_QUALITY_EVIDENCE_OWNER_DOWNLOAD_LOG_CANDIDATE,
    ALBUM_QUALITY_EVIDENCE_OWNER_IMPORT_JOB_CANDIDATE,
    ALBUM_QUALITY_EVIDENCE_OWNER_REQUEST_CURRENT,
    AudioQualityMeasurement,
)
from lib.quality_evidence import snapshot_audio_files
from tests.helpers import (
    make_album_quality_evidence,
    make_import_result,
    make_request_row,
    patch_dispatch_externals,
)
from tests.fakes import FakePipelineDB


class TestDispatchFromDbOrchestration(unittest.TestCase):
    """Orchestration tests — assert domain state after force/manual import."""

    def _dispatch(self, force=True, ir=None, outcome_label=None,
                  source_username=None,
                  **req_overrides):
        from lib.import_dispatch import dispatch_import_from_db

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

        if ir is None:
            ir = make_import_result(decision="import", new_min_bitrate=320)
        if outcome_label is None:
            outcome_label = "force_import" if force else "manual_import"

        tmpdir = tempfile.mkdtemp()
        try:
            with patch_dispatch_externals() as ext, \
                 patch("lib.import_dispatch._check_quality_gate_core") as mock_gate, \
                 patch("lib.import_dispatch.parse_import_result", return_value=ir), \
                 patch("lib.config.read_runtime_config",
                       return_value=CratediggerConfig(
                           beets_harness_path="/nix/store/fake/harness/run_beets_harness.sh",
                           pipeline_db_enabled=True,
                       )):
                result = dispatch_import_from_db(
                    db, request_id=42, failed_path=tmpdir,  # type: ignore[arg-type]
                    force=force, source_username=source_username,
                    outcome_label=outcome_label,
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
            "mock_meelo": ext.meelo,
            "mock_jellyfin": ext.jellyfin,
            "mock_cleanup": ext.cleanup,
        }

    # --- Success path ---

    def test_successful_force_import_marks_imported(self):
        r = self._dispatch()
        self.assertTrue(r["result"].success)
        self.assertEqual(r["db"].request(42)["status"], "imported")

    def test_success_logs_with_force_import_outcome(self):
        r = self._dispatch()
        logs = r["db"].download_logs
        self.assertEqual(len(logs), 1)
        self.assertEqual(logs[0].outcome, "force_import")

    def test_successful_force_and_manual_imports_run_post_import_pipeline(self):
        for force in (True, False):
            with self.subTest(force=force):
                r = self._dispatch(force=force)
                r["mock_gate"].assert_called_once()
                r["mock_meelo"].assert_called_once()
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
        r = self._dispatch(force=True)
        self.assertIn("--force", r["cmd"])

    def test_no_force_for_manual_import(self):
        r = self._dispatch(force=False)
        self.assertNotIn("--force", r["cmd"])

    def test_force_import_command_has_no_preview_import_result_channel(self):
        r = self._dispatch(
            force=True,
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
        from lib.import_dispatch import dispatch_import_from_db

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            mb_release_id="mbid-123",
            status="manual",
            artist_name="Son Ambulance",
            album_title="Someone Else's Deja Vu",
        ))
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
            db.upsert_album_quality_evidence(make_album_quality_evidence(
                owner_type=ALBUM_QUALITY_EVIDENCE_OWNER_DOWNLOAD_LOG_CANDIDATE,
                owner_id=download_log_id,
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
            ))
            db.upsert_album_quality_evidence(make_album_quality_evidence(
                owner_type=ALBUM_QUALITY_EVIDENCE_OWNER_REQUEST_CURRENT,
                owner_id=42,
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
            ))
            with patch_dispatch_externals() as ext, \
                 patch("lib.import_dispatch._check_quality_gate_core"), \
                 patch("lib.import_dispatch.parse_import_result", return_value=ir), \
                 patch("lib.import_dispatch.repair_mp3_headers") as repair, \
                 patch("lib.import_dispatch.inspect_local_files") as inspect, \
                 patch("lib.import_dispatch.run_preimport_gates") as gates, \
                 patch("lib.config.read_runtime_config",
                       return_value=CratediggerConfig(
                           beets_harness_path="/nix/store/fake/harness/run_beets_harness.sh",
                           pipeline_db_enabled=True,
                       )):
                result = dispatch_import_from_db(
                    db,  # type: ignore[arg-type]
                    request_id=42,
                    failed_path=tmpdir,  # type: ignore[arg-type]
                    force=True,
                    source_username="alice",
                    download_log_id=download_log_id,
                )

            self.assertTrue(result.success)
            repair.assert_not_called()
            inspect.assert_not_called()
            gates.assert_not_called()
            cmd = ext.run.call_args[0][0]
            self.assertIn("--quality-evidence-action-file", cmd)
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_manual_import_with_valid_candidate_evidence_skips_preimport_measurement(self):
        from lib.import_dispatch import dispatch_import_from_db

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            mb_release_id="mbid-123",
            status="manual",
            artist_name="Son Ambulance",
            album_title="Someone Else's Deja Vu",
        ))
        ir = make_import_result(decision="import", new_min_bitrate=245)
        tmpdir = tempfile.mkdtemp()
        try:
            with open(os.path.join(tmpdir, "01.mp3"), "wb") as handle:
                handle.write(b"audio")
            job = db.enqueue_import_job(
                IMPORT_JOB_MANUAL,
                request_id=42,
                payload=manual_import_payload(failed_path=tmpdir),
                preview_enabled=True,
            )
            import_job_id = job.id
            files = snapshot_audio_files(tmpdir)
            db.upsert_album_quality_evidence(make_album_quality_evidence(
                owner_type=ALBUM_QUALITY_EVIDENCE_OWNER_IMPORT_JOB_CANDIDATE,
                owner_id=import_job_id,
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
            ))
            db.upsert_album_quality_evidence(make_album_quality_evidence(
                owner_type=ALBUM_QUALITY_EVIDENCE_OWNER_REQUEST_CURRENT,
                owner_id=42,
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
            ))
            with patch_dispatch_externals() as ext, \
                 patch("lib.import_dispatch._check_quality_gate_core"), \
                 patch("lib.import_dispatch.parse_import_result", return_value=ir), \
                 patch("lib.import_dispatch.repair_mp3_headers") as repair, \
                 patch("lib.import_dispatch.inspect_local_files") as inspect, \
                 patch("lib.import_dispatch.run_preimport_gates") as gates, \
                 patch("lib.config.read_runtime_config",
                       return_value=CratediggerConfig(
                           beets_harness_path="/nix/store/fake/harness/run_beets_harness.sh",
                           pipeline_db_enabled=True,
                       )):
                result = dispatch_import_from_db(
                    db,  # type: ignore[arg-type]
                    request_id=42,
                    failed_path=tmpdir,  # type: ignore[arg-type]
                    force=False,
                    source_username="alice",
                    import_job_id=import_job_id,
                    outcome_label="manual_import",
                )

            self.assertTrue(result.success)
            repair.assert_not_called()
            inspect.assert_not_called()
            gates.assert_not_called()
            cmd = ext.run.call_args[0][0]
            self.assertIn("--quality-evidence-action-file", cmd)
            self.assertNotIn("--force", cmd)
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_force_import_with_stale_candidate_evidence_fails_before_preimport(self):
        from lib.import_dispatch import dispatch_import_from_db

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            mb_release_id="mbid-123",
            status="manual",
            artist_name="Son Ambulance",
            album_title="Someone Else's Deja Vu",
        ))
        download_log_id = db.log_download(
            42,
            outcome="rejected",
            validation_result={"failed_path": ""},
        )
        tmpdir = tempfile.mkdtemp()
        try:
            track = os.path.join(tmpdir, "01.mp3")
            with open(track, "wb") as handle:
                handle.write(b"audio")
            db.upsert_album_quality_evidence(make_album_quality_evidence(
                owner_type=ALBUM_QUALITY_EVIDENCE_OWNER_DOWNLOAD_LOG_CANDIDATE,
                owner_id=download_log_id,
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
            ))
            with open(track, "ab") as handle:
                handle.write(b" changed")

            with patch_dispatch_externals() as ext, \
                 patch("lib.import_dispatch.repair_mp3_headers") as repair, \
                 patch("lib.import_dispatch.inspect_local_files") as inspect, \
                 patch("lib.import_dispatch.run_preimport_gates") as gates, \
                 patch("lib.config.read_runtime_config",
                       return_value=CratediggerConfig(
                           beets_harness_path="/nix/store/fake/harness/run_beets_harness.sh",
                           pipeline_db_enabled=True,
                       )):
                result = dispatch_import_from_db(
                    db,  # type: ignore[arg-type]
                    request_id=42,
                    failed_path=tmpdir,  # type: ignore[arg-type]
                    force=True,
                    source_username="alice",
                    download_log_id=download_log_id,
                )

            self.assertFalse(result.success)
            self.assertIn("Candidate quality evidence unavailable", result.message)
            repair.assert_not_called()
            inspect.assert_not_called()
            gates.assert_not_called()
            ext.run.assert_not_called()
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)

    # --- Seam: preserve-source flag (issue #111) ---

    def test_preserve_source_flag_passed_on_force(self):
        """Force-import must preserve user's source FLACs until the quality
        decision — downgrade/transcode_downgrade verdicts must NOT destroy
        originals in failed_imports/."""
        r = self._dispatch(force=True)
        self.assertIn("--preserve-source", r["cmd"])

    def test_preserve_source_flag_passed_on_manual(self):
        """Manual-import uses the same failed_imports/ source path as force —
        both need source preservation."""
        r = self._dispatch(force=False)
        self.assertIn("--preserve-source", r["cmd"])

    # --- Typed result ---

    def test_returns_typed_result(self):
        r = self._dispatch()
        self.assertTrue(hasattr(r["result"], "success"))
        self.assertTrue(hasattr(r["result"], "message"))

    # --- Issue #89: force/manual rejections must NOT delete source files ---
    #
    # Auto-import passes a disposable /Incoming staging directory — cleanup
    # on `downgrade` / `transcode_downgrade` is correct. Force/manual pass
    # the user's `failed_imports/…` directory, which IS the only copy of
    # the source material. A cleanup there would delete the user's data
    # when the harness decides against importing.

    def test_force_downgrade_does_not_delete_source(self):
        """Issue #89: downgrade decision on force-import must not rmtree
        the failed_imports source directory."""
        ir = make_import_result(decision="downgrade",
                                new_min_bitrate=128, prev_min_bitrate=180)
        r = self._dispatch(force=True, ir=ir)
        r["mock_cleanup"].assert_not_called()

    def test_manual_downgrade_does_not_delete_source(self):
        """Issue #89: downgrade decision on manual-import must not rmtree
        the failed_imports source directory."""
        ir = make_import_result(decision="downgrade",
                                new_min_bitrate=128, prev_min_bitrate=180)
        r = self._dispatch(force=False, ir=ir)
        r["mock_cleanup"].assert_not_called()

    def test_force_transcode_downgrade_does_not_delete_source(self):
        """Issue #89: transcode_downgrade on force-import must not rmtree."""
        ir = make_import_result(decision="transcode_downgrade",
                                new_min_bitrate=190, prev_min_bitrate=320)
        r = self._dispatch(force=True, ir=ir)
        r["mock_cleanup"].assert_not_called()

    def test_manual_transcode_downgrade_does_not_delete_source(self):
        """Issue #89: transcode_downgrade on manual-import must not rmtree."""
        ir = make_import_result(decision="transcode_downgrade",
                                new_min_bitrate=190, prev_min_bitrate=320)
        r = self._dispatch(force=False, ir=ir)
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
        r = self._dispatch(force=True)  # default decision="import"
        self.assertTrue(r["result"].success)
        r["mock_cleanup"].assert_called_once_with(r["path"])

    def test_manual_import_success_cleans_empty_source(self):
        """Same invariant for manual-import: successful import cleans the
        now-empty source folder so the wrong-matches tab reflects reality.
        """
        r = self._dispatch(force=False)
        self.assertTrue(r["result"].success)
        r["mock_cleanup"].assert_called_once_with(r["path"])


class TestDispatchFromDbAdvisoryLock(unittest.TestCase):
    """Issue #92: concurrent force/manual-import on the same request_id
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
        return db

    def _dispatch(self, db: "FakePipelineDB"):
        from lib.import_dispatch import dispatch_import_from_db
        ir = make_import_result(decision="import", new_min_bitrate=320)
        tmpdir = tempfile.mkdtemp()
        try:
            with patch_dispatch_externals() as ext, \
                 patch("lib.import_dispatch._check_quality_gate_core"), \
                 patch("lib.import_dispatch.parse_import_result", return_value=ir), \
                 patch("lib.config.read_runtime_config",
                       return_value=CratediggerConfig(
                           beets_harness_path="/nix/store/fake/harness/run_beets_harness.sh",
                           pipeline_db_enabled=True,
                       )):
                result = dispatch_import_from_db(
                    db, request_id=42, failed_path=tmpdir,  # type: ignore[arg-type]
                    force=True,
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

    def test_contention_skips_preimport_gates(self):
        """Contended call must not even run inspect_local_files / run_preimport_gates."""
        db = self._seed_db()
        db.set_advisory_lock_result(False)
        tmpdir = tempfile.mkdtemp()
        try:
            from lib.import_dispatch import dispatch_import_from_db
            with patch("lib.import_dispatch.run_preimport_gates") as mock_gates, \
                 patch("lib.import_dispatch.inspect_local_files") as mock_inspect, \
                 patch("lib.config.read_runtime_config",
                       return_value=CratediggerConfig(
                           beets_harness_path="/nix/store/fake/harness/run_beets_harness.sh",
                           pipeline_db_enabled=True,
                       )):
                result = dispatch_import_from_db(
                    db, request_id=42, failed_path=tmpdir,  # type: ignore[arg-type]
                    force=True,
                )
            self.assertFalse(result.success)
            mock_gates.assert_not_called()
            mock_inspect.assert_not_called()
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)


class TestDispatchFromDbRuntimeConfigSeam(unittest.TestCase):
    def test_dispatch_import_from_db_uses_shared_runtime_config_reader(self):
        from lib.import_dispatch import dispatch_import_from_db

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            mb_release_id="mbid-123",
            status="manual",
            artist_name="Artist",
            album_title="Album",
        ))

        cfg = CratediggerConfig(
            beets_harness_path="/nix/store/fake/harness/run_beets_harness.sh",
            pipeline_db_enabled=True,
        )
        ir = make_import_result(decision="import", new_min_bitrate=320)
        tmpdir = tempfile.mkdtemp()
        try:
            with patch_dispatch_externals() as ext, \
                 patch("lib.import_dispatch._check_quality_gate_core"), \
                 patch("lib.import_dispatch.parse_import_result", return_value=ir), \
                 patch("lib.config.read_runtime_config", return_value=cfg) as mock_read:
                dispatch_import_from_db(
                    db,  # type: ignore[arg-type]
                    request_id=42,
                    failed_path=tmpdir,
                    force=True,
                )
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)

        mock_read.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
