"""Tests for dispatch_import_core — orchestration outcomes with FakePipelineDB.

Orchestration tests assert domain state: request status, download_log rows,
denylist entries, requeue behavior. Seam tests (argv, flag forwarding) are
in a separate class and explicitly labeled.
"""

import tempfile
import unittest
from unittest.mock import MagicMock, patch

from lib.config import CratediggerConfig
from lib.quality import DownloadInfo
from tests.fakes import FakePipelineDB
from tests.helpers import make_import_result, make_request_row, patch_dispatch_externals


_HARNESS = "/nix/store/fake/harness/run_beets_harness.sh"


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


if __name__ == "__main__":
    unittest.main()
