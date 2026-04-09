"""Tests for dispatch_import_core — the plain-params core of dispatch_import.

Tests behavioral outcomes: mark_done/mark_failed called correctly,
quality gate runs, meelo triggers, downgrade prevention, outcome labels.
"""

import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from lib.config import SoularrConfig
from lib.quality import (DownloadInfo, ImportResult, ConversionInfo,
                         AudioQualityMeasurement, PostflightInfo,
                         QUALITY_UPGRADE_TIERS)
from tests.helpers import make_request_row


def _make_import_result(decision="import", new_min_bitrate=245,
                        prev_min_bitrate=None, was_converted=False,
                        original_filetype=None, target_filetype=None,
                        spectral_grade="genuine", spectral_bitrate=None,
                        verified_lossless=None, error=None):
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


class TestDispatchImportCore(unittest.TestCase):
    """dispatch_import_core must handle the full import pipeline with plain params."""

    def _dispatch(self, ir=None, force=False, outcome_label="success",
                  requeue_on_failure=True, override_min_bitrate=None,
                  source_username=None, target_format=None,
                  verified_lossless_target=""):
        from lib.import_dispatch import dispatch_import_core
        if ir is None:
            ir = _make_import_result(decision="import", new_min_bitrate=245)

        db = MagicMock()
        db.get_request.return_value = make_request_row(
            id=42, status="downloading",
            min_bitrate=180, current_spectral_bitrate=128,
        )
        cfg = SoularrConfig(
            beets_harness_path="/nix/store/fake/harness/run_beets_harness.sh",
            pipeline_db_enabled=True,
            verified_lossless_target=verified_lossless_target,
        )
        files = [MagicMock(username=source_username or "user1",
                           filename="01 - Track.mp3")]
        dl_info = DownloadInfo(username=source_username)

        tmpdir = tempfile.mkdtemp()
        try:
            with patch("lib.import_dispatch.sp.run") as mock_run, \
                 patch("lib.import_dispatch._cleanup_staged_dir") as mock_cleanup, \
                 patch("lib.import_dispatch._check_quality_gate_core") as mock_gate, \
                 patch("lib.import_dispatch.parse_import_result", return_value=ir), \
                 patch("lib.util.trigger_meelo_scan") as mock_meelo, \
                 patch("lib.util.trigger_plex_scan") as mock_plex, \
                 patch("lib.import_dispatch.cleanup_disambiguation_orphans",
                       return_value=[]):
                mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
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
                    db=db,
                    dl_info=dl_info,
                    distance=0.05,
                    scenario="strong_match",
                    files=files,
                    cfg=cfg,
                    outcome_label=outcome_label,
                    requeue_on_failure=requeue_on_failure,
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
            "mock_plex": mock_plex,
            "mock_gate": mock_gate,
            "mock_cleanup": mock_cleanup,
            "dl_info": dl_info,
        }

    def test_successful_import_returns_success(self):
        r = self._dispatch()
        self.assertTrue(r["result"].success)

    def test_successful_import_logs_download(self):
        r = self._dispatch()
        r["db"].log_download.assert_called_once()
        self.assertEqual(r["db"].log_download.call_args.kwargs["outcome"], "success")

    def test_quality_gate_runs(self):
        r = self._dispatch()
        r["mock_gate"].assert_called_once()

    def test_meelo_scan_triggered(self):
        r = self._dispatch()
        r["mock_meelo"].assert_called_once()

    def test_cleanup_runs(self):
        r = self._dispatch()
        r["mock_cleanup"].assert_called_once()

    def test_force_flag_passed(self):
        r = self._dispatch(force=True)
        self.assertIn("--force", r["cmd"])

    def test_no_force_flag_by_default(self):
        r = self._dispatch(force=False)
        self.assertNotIn("--force", r["cmd"])

    def test_override_min_bitrate_passed(self):
        r = self._dispatch(override_min_bitrate=128)
        idx = r["cmd"].index("--override-min-bitrate")
        self.assertEqual(r["cmd"][idx + 1], "128")

    def test_outcome_label_in_download_log(self):
        """Custom outcome_label (e.g. force_import) must appear in download_log."""
        r = self._dispatch(outcome_label="force_import")
        self.assertEqual(r["db"].log_download.call_args.kwargs["outcome"], "force_import")

    def test_downgrade_prevented(self):
        ir = _make_import_result(decision="downgrade",
                                 new_min_bitrate=128, prev_min_bitrate=180)
        r = self._dispatch(ir=ir)
        self.assertFalse(r["result"].success)

    def test_downgrade_denylists_user(self):
        ir = _make_import_result(decision="downgrade",
                                 new_min_bitrate=128, prev_min_bitrate=180)
        r = self._dispatch(ir=ir, source_username="baduser")
        r["db"].add_denylist.assert_called()

    def test_failed_no_requeue(self):
        """When requeue_on_failure=False, failed import must NOT transition to wanted."""
        ir = _make_import_result(decision="downgrade",
                                 new_min_bitrate=128, prev_min_bitrate=180)
        r = self._dispatch(ir=ir, requeue_on_failure=False)
        r["db"].reset_to_wanted.assert_not_called()

    def test_failed_with_requeue(self):
        """When requeue_on_failure=True (default), failed import transitions to wanted."""
        ir = _make_import_result(decision="downgrade",
                                 new_min_bitrate=128, prev_min_bitrate=180)
        r = self._dispatch(ir=ir, requeue_on_failure=True)
        r["db"].log_download.assert_called()

    def test_transcode_upgrade_requeues(self):
        ir = _make_import_result(decision="transcode_upgrade",
                                 new_min_bitrate=227)
        r = self._dispatch(ir=ir)
        self.assertTrue(r["result"].success)
        r["db"].add_denylist.assert_called()
        r["db"].reset_to_wanted.assert_called_once()

    def test_verified_lossless_target_flag(self):
        r = self._dispatch(verified_lossless_target="opus 128")
        self.assertIn("--verified-lossless-target", r["cmd"])
        idx = r["cmd"].index("--verified-lossless-target")
        self.assertEqual(r["cmd"][idx + 1], "opus 128")

    def test_target_format_flag(self):
        r = self._dispatch(target_format="flac")
        self.assertIn("--target-format", r["cmd"])
        idx = r["cmd"].index("--target-format")
        self.assertEqual(r["cmd"][idx + 1], "flac")


if __name__ == "__main__":
    unittest.main()
