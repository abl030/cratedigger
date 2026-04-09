"""Tests for dispatch_import_from_db — force/manual import through the real pipeline.

RED tests: these test a function that doesn't exist yet. They verify that
force-import and manual-import go through the same decision pipeline as
auto-import (dispatch_action, quality gate, downgrade prevention, meelo scan).
"""

import unittest
from unittest.mock import MagicMock, patch

from lib.quality import (DownloadInfo, ImportResult, ConversionInfo,
                         AudioQualityMeasurement, PostflightInfo,
                         SpectralMeasurement)
from tests.helpers import make_request_row


def _make_import_result(decision="import", new_min_bitrate=245,
                        prev_min_bitrate=None, was_converted=False,
                        original_filetype=None, target_filetype=None,
                        spectral_grade="genuine", spectral_bitrate=None,
                        verified_lossless=None, error=None):
    """Build an ImportResult for testing."""
    if verified_lossless is None:
        verified_lossless = was_converted and spectral_grade == "genuine"
    return ImportResult(
        decision=decision,
        error=error,
        new_measurement=AudioQualityMeasurement(
            min_bitrate_kbps=new_min_bitrate,
            spectral_grade=spectral_grade,
            spectral_bitrate_kbps=spectral_bitrate,
            verified_lossless=verified_lossless,
            was_converted_from=original_filetype if was_converted else None,
        ),
        existing_measurement=(AudioQualityMeasurement(min_bitrate_kbps=prev_min_bitrate)
                              if prev_min_bitrate is not None else None),
        conversion=ConversionInfo(
            was_converted=was_converted,
            original_filetype=original_filetype or "",
            target_filetype=target_filetype or "",
        ),
        postflight=PostflightInfo(),
    )


class TestDispatchImportForceFlag(unittest.TestCase):
    """dispatch_import must pass --force when force=True."""

    def test_force_flag_in_command(self):
        from lib.import_dispatch import dispatch_import
        album_data = MagicMock()
        album_data.artist = "Test"
        album_data.title = "Album"
        album_data.mb_release_id = "mbid-123"
        album_data.db_request_id = 42
        album_data.db_target_format = None
        album_data.files = []
        ctx = MagicMock()
        ctx.cfg.beets_harness_path = "/nix/store/fake/harness/run_beets_harness.sh"
        ctx.cfg.verified_lossless_target = ""
        ctx.cfg.pipeline_db_enabled = True
        ctx.cooled_down_users = set()
        db_mock = MagicMock()
        db_mock.get_request.return_value = make_request_row(min_bitrate=200)
        ctx.pipeline_db_source._get_db.return_value = db_mock
        bv_result = MagicMock(distance=0.22, scenario="wrong_match")
        dl_info = DownloadInfo()
        ir = _make_import_result(decision="import")

        with patch("lib.import_dispatch.sp.run") as mock_run, \
             patch("lib.import_dispatch._cleanup_staged_dir"), \
             patch("lib.import_dispatch.trigger_meelo_scan"), \
             patch("lib.import_dispatch._check_quality_gate"), \
             patch("lib.import_dispatch.parse_import_result", return_value=ir):
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            dispatch_import(album_data, bv_result, "/tmp/dest", dl_info,
                            42, ctx, force=True)
            cmd = mock_run.call_args[0][0]

        self.assertIn("--force", cmd)

    def test_no_force_flag_by_default(self):
        from lib.import_dispatch import dispatch_import
        album_data = MagicMock()
        album_data.artist = "Test"
        album_data.title = "Album"
        album_data.mb_release_id = "mbid-123"
        album_data.db_request_id = 42
        album_data.db_target_format = None
        album_data.files = []
        ctx = MagicMock()
        ctx.cfg.beets_harness_path = "/nix/store/fake/harness/run_beets_harness.sh"
        ctx.cfg.verified_lossless_target = ""
        ctx.cfg.pipeline_db_enabled = True
        ctx.cooled_down_users = set()
        db_mock = MagicMock()
        db_mock.get_request.return_value = make_request_row(min_bitrate=200)
        ctx.pipeline_db_source._get_db.return_value = db_mock
        bv_result = MagicMock(distance=0.05, scenario="strong_match")
        dl_info = DownloadInfo()
        ir = _make_import_result(decision="import")

        with patch("lib.import_dispatch.sp.run") as mock_run, \
             patch("lib.import_dispatch._cleanup_staged_dir"), \
             patch("lib.import_dispatch.trigger_meelo_scan"), \
             patch("lib.import_dispatch._check_quality_gate"), \
             patch("lib.import_dispatch.parse_import_result", return_value=ir):
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            dispatch_import(album_data, bv_result, "/tmp/dest", dl_info,
                            42, ctx)
            cmd = mock_run.call_args[0][0]

        self.assertNotIn("--force", cmd)


class TestDispatchImportFromDb(unittest.TestCase):
    """dispatch_import_from_db should run the full pipeline for force/manual import."""

    def _make_db(self, request_row=None, **req_overrides):
        db = MagicMock()
        if request_row is None:
            request_row = make_request_row(
                id=42, mb_release_id="mbid-123",
                artist_name="Son Ambulance",
                album_title="Someone Else's Deja Vu",
                min_bitrate=180, current_spectral_bitrate=128,
                current_spectral_grade="likely_transcode",
                **req_overrides,
            )
        db.get_request.return_value = request_row
        return db

    def _dispatch(self, db=None, force=True, ir=None, **req_overrides):
        from lib.import_dispatch import dispatch_import_from_db
        if db is None:
            db = self._make_db(**req_overrides)
        if ir is None:
            ir = _make_import_result(decision="import", new_min_bitrate=320)

        import tempfile
        tmpdir = tempfile.mkdtemp()
        try:
            with patch("lib.import_dispatch.sp.run") as mock_run, \
                 patch("lib.import_dispatch._cleanup_staged_dir"), \
                 patch("lib.import_dispatch.trigger_meelo_scan") as mock_meelo, \
                 patch("lib.import_dispatch._check_quality_gate") as mock_gate, \
                 patch("lib.import_dispatch.parse_import_result", return_value=ir), \
                 patch("lib.import_dispatch.trigger_plex_scan"), \
                 patch("lib.import_dispatch.cleanup_disambiguation_orphans", return_value=[]), \
                 patch("lib.import_dispatch._read_minimal_config", return_value={
                     "beets_harness_path": "/nix/store/fake/harness/run_beets_harness.sh",
                     "verified_lossless_target": "",
                 }):
                mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
                result = dispatch_import_from_db(
                    db, request_id=42, failed_path=tmpdir,
                    force=force,
                )
                cmd = mock_run.call_args[0][0] if mock_run.call_args else []
        finally:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)

        return {
            "result": result,
            "cmd": cmd,
            "db": db,
            "mock_meelo": mock_meelo,
            "mock_gate": mock_gate,
        }

    def test_uses_effective_override_bitrate(self):
        """Must use min(min_bitrate, spectral_bitrate) as override.

        This is the exact bug: Son Ambulance had min_bitrate=180, spectral=128.
        The old path passed min_bitrate=180 directly. The correct behavior is
        to pass 128 (spectral is lower = more conservative).
        """
        r = self._dispatch()
        cmd = r["cmd"]
        idx = cmd.index("--override-min-bitrate")
        override_value = int(cmd[idx + 1])
        self.assertEqual(override_value, 128,
                         "Should use spectral bitrate (128) since it's lower than min_bitrate (180)")

    def test_force_flag_passed(self):
        r = self._dispatch(force=True)
        self.assertIn("--force", r["cmd"])

    def test_no_force_for_manual_import(self):
        r = self._dispatch(force=False)
        self.assertNotIn("--force", r["cmd"])

    def test_quality_gate_runs(self):
        """Quality gate must run after successful force-import."""
        r = self._dispatch()
        r["mock_gate"].assert_called_once()

    def test_meelo_scan_triggered(self):
        """Meelo scan must trigger after successful force-import."""
        r = self._dispatch()
        r["mock_meelo"].assert_called_once()

    def test_downgrade_prevented(self):
        """Force-import of a downgrade should still be rejected by dispatch_action."""
        ir = _make_import_result(decision="downgrade",
                                 new_min_bitrate=128, prev_min_bitrate=180)
        r = self._dispatch(ir=ir)
        # Result should indicate failure (downgrade prevented)
        self.assertFalse(r["result"].success)

    def test_returns_typed_result(self):
        """Must return a typed result with success, message, exit_code."""
        r = self._dispatch()
        result = r["result"]
        self.assertTrue(hasattr(result, "success"))
        self.assertTrue(hasattr(result, "message"))


if __name__ == "__main__":
    unittest.main()
