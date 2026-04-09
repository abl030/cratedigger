"""Tests for dispatch_import_from_db — force/manual import through the real pipeline.

Orchestration tests use FakePipelineDB to assert domain state (request status,
log rows, denylist). Seam tests verify argv/config wiring.
"""

import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from lib.config import SoularrConfig
from tests.helpers import make_request_row, make_import_result
from tests.fakes import FakePipelineDB


class TestDispatchFromDbOrchestration(unittest.TestCase):
    """Orchestration tests — assert domain state after force/manual import."""

    def _dispatch(self, force=True, ir=None,
                  source_username=None, **req_overrides):
        from lib.import_dispatch import dispatch_import_from_db

        db = FakePipelineDB()
        req = make_request_row(
            id=42, mb_release_id="mbid-123",
            status="downloading",
            artist_name="Son Ambulance",
            album_title="Someone Else's Deja Vu",
            min_bitrate=180, current_spectral_bitrate=128,
            current_spectral_grade="likely_transcode",
            **req_overrides,
        )
        db.seed_request(req)

        if ir is None:
            ir = make_import_result(decision="import", new_min_bitrate=320)

        tmpdir = tempfile.mkdtemp()
        try:
            with patch("lib.import_dispatch.sp.run") as mock_run, \
                 patch("lib.import_dispatch._cleanup_staged_dir"), \
                 patch("lib.util.trigger_meelo_scan"), \
                 patch("lib.import_dispatch._check_quality_gate_core"), \
                 patch("lib.import_dispatch.parse_import_result", return_value=ir), \
                 patch("lib.util.trigger_plex_scan"), \
                 patch("lib.import_dispatch.cleanup_disambiguation_orphans",
                       return_value=[]), \
                 patch("lib.import_dispatch._read_runtime_config",
                       return_value=SoularrConfig(
                           beets_harness_path="/nix/store/fake/harness/run_beets_harness.sh",
                           pipeline_db_enabled=True,
                       )):
                mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
                result = dispatch_import_from_db(
                    db, request_id=42, failed_path=tmpdir,  # type: ignore[arg-type]
                    force=force, source_username=source_username,
                )
                cmd = mock_run.call_args[0][0] if mock_run.call_args else []
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

    def test_successful_force_import_marks_imported(self):
        r = self._dispatch()
        self.assertTrue(r["result"].success)
        self.assertEqual(r["db"].request(42)["status"], "imported")

    def test_success_logs_with_force_import_outcome(self):
        r = self._dispatch()
        logs = r["db"].download_logs
        self.assertEqual(len(logs), 1)
        self.assertEqual(logs[0].outcome, "force_import")

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
        self.assertNotEqual(r["db"].request(42)["status"], "wanted")

    def test_transcode_downgrade_does_not_requeue(self):
        ir = make_import_result(decision="transcode_downgrade",
                                new_min_bitrate=190, prev_min_bitrate=320)
        r = self._dispatch(ir=ir)
        self.assertNotEqual(r["db"].request(42)["status"], "wanted")

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

    # --- Typed result ---

    def test_returns_typed_result(self):
        r = self._dispatch()
        self.assertTrue(hasattr(r["result"], "success"))
        self.assertTrue(hasattr(r["result"], "message"))


class TestReadRuntimeConfig(unittest.TestCase):
    def test_missing_config_returns_default(self):
        from lib.import_dispatch import _read_runtime_config
        with patch.dict(os.environ, {"SOULARR_RUNTIME_CONFIG": "/nonexistent/config.ini"}):
            cfg = _read_runtime_config()
        self.assertEqual(cfg.beets_harness_path, "")

    def test_reads_full_config(self):
        from lib.import_dispatch import _read_runtime_config
        with tempfile.NamedTemporaryFile("w", delete=False, suffix=".ini") as tmp:
            tmp.write(
                "[Beets Validation]\n"
                "harness_path = /nix/store/test/run_beets_harness.sh\n"
                "verified_lossless_target = opus 128\n"
                "[Meelo]\n"
                "url = http://meelo.test\n"
                "[Plex]\n"
                "url = http://plex.test\n"
                "token = test-token\n"
            )
            config_path = tmp.name
        try:
            with patch.dict(os.environ, {"SOULARR_RUNTIME_CONFIG": config_path}):
                cfg = _read_runtime_config()
        finally:
            os.unlink(config_path)

        self.assertEqual(cfg.beets_harness_path, "/nix/store/test/run_beets_harness.sh")
        self.assertEqual(cfg.verified_lossless_target, "opus 128")
        self.assertEqual(cfg.meelo_url, "http://meelo.test")
        self.assertEqual(cfg.plex_url, "http://plex.test")
        self.assertEqual(cfg.plex_token, "test-token")


if __name__ == "__main__":
    unittest.main()
