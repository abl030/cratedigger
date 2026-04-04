"""Tests for lib/import_service.py — unified import service."""

import json
import unittest
from unittest.mock import MagicMock, patch

from lib.import_service import (
    ImportOutcome,
    parse_import_result_stdout,
    extract_import_update_fields,
    extract_import_log_fields,
    run_import,
    log_and_update_import,
)


class TestParseImportResultStdout(unittest.TestCase):
    def test_extracts_json(self):
        stdout = 'some log\n__IMPORT_RESULT__{"decision":"import"}\nmore log'
        result = parse_import_result_stdout(stdout)
        self.assertIsNotNone(result)
        data = json.loads(result)  # type: ignore
        self.assertEqual(data["decision"], "import")

    def test_no_sentinel(self):
        self.assertIsNone(parse_import_result_stdout("no sentinel here"))

    def test_empty(self):
        self.assertIsNone(parse_import_result_stdout(""))


class TestExtractImportUpdateFields(unittest.TestCase):
    def test_v2_format(self):
        ir = json.dumps({
            "new_measurement": {
                "spectral_grade": "genuine",
                "spectral_bitrate_kbps": 256,
                "min_bitrate_kbps": 245,
                "verified_lossless": True,
            }
        })
        fields = extract_import_update_fields(ir)
        self.assertEqual(fields["spectral_grade"], "genuine")
        self.assertEqual(fields["spectral_bitrate"], 256)
        self.assertEqual(fields["min_bitrate"], 245)
        self.assertTrue(fields["verified_lossless"])

    def test_v1_format(self):
        ir = json.dumps({
            "spectral": {"grade": "genuine", "bitrate": 256},
            "quality": {"new_min_bitrate": 245},
            "conversion": {
                "was_converted": True,
                "original_filetype": "flac",
            },
        })
        fields = extract_import_update_fields(ir)
        self.assertEqual(fields["spectral_grade"], "genuine")
        self.assertEqual(fields["spectral_bitrate"], 256)
        self.assertEqual(fields["min_bitrate"], 245)
        self.assertTrue(fields["verified_lossless"])

    def test_none_returns_empty(self):
        self.assertEqual(extract_import_update_fields(None), {})

    def test_invalid_json_returns_empty(self):
        self.assertEqual(extract_import_update_fields("not json"), {})


class TestExtractImportLogFields(unittest.TestCase):
    def test_converted_flac(self):
        from lib.quality import ImportResult, ConversionInfo, AudioQualityMeasurement, PostflightInfo
        ir = ImportResult(
            decision="import",
            new_measurement=AudioQualityMeasurement(min_bitrate_kbps=245, spectral_grade="genuine"),
            conversion=ConversionInfo(was_converted=True, original_filetype="flac", target_filetype="mp3"),
            postflight=PostflightInfo(),
        )
        fields = extract_import_log_fields(ir.to_json())
        self.assertTrue(fields["was_converted"])
        self.assertEqual(fields["slskd_filetype"], "flac")
        self.assertEqual(fields["actual_filetype"], "mp3")
        self.assertEqual(fields["bitrate"], 245000)

    def test_none_returns_empty(self):
        self.assertEqual(extract_import_log_fields(None), {})


class TestRunImport(unittest.TestCase):
    def test_path_not_found(self):
        result = run_import("/nonexistent/path", "mbid",
                            request_id=1, import_one_path="/fake")
        self.assertFalse(result.success)
        self.assertEqual(result.exit_code, 3)

    @patch("lib.import_service.subprocess.run")
    @patch("lib.import_service.os.path.isdir", return_value=True)
    def test_force_flag_passed(self, _isdir, mock_run):
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        run_import("/tmp/test", "mbid", request_id=1,
                   import_one_path="/fake/import_one.py", force=True)
        cmd = mock_run.call_args[0][0]
        self.assertIn("--force", cmd)

    @patch("lib.import_service.subprocess.run")
    @patch("lib.import_service.os.path.isdir", return_value=True)
    def test_no_force_flag(self, _isdir, mock_run):
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        run_import("/tmp/test", "mbid", request_id=1,
                   import_one_path="/fake/import_one.py", force=False)
        cmd = mock_run.call_args[0][0]
        self.assertNotIn("--force", cmd)

    @patch("lib.import_service.subprocess.run")
    @patch("lib.import_service.os.path.isdir", return_value=True)
    def test_override_min_bitrate(self, _isdir, mock_run):
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        run_import("/tmp/test", "mbid", request_id=1,
                   import_one_path="/fake", override_min_bitrate=245)
        cmd = mock_run.call_args[0][0]
        self.assertIn("--override-min-bitrate", cmd)
        idx = cmd.index("--override-min-bitrate")
        self.assertEqual(cmd[idx + 1], "245")


class TestLogAndUpdateImport(unittest.TestCase):
    def test_success_logs_and_updates(self):
        db = MagicMock()
        db.get_request.return_value = {"status": "manual"}
        outcome = ImportOutcome(success=True, exit_code=0, message="ok",
                                import_result_json='{"decision":"import","new_measurement":{"min_bitrate_kbps":245}}')
        log_and_update_import(db, 42, outcome,
                              outcome_label="force_import",
                              staged_path="/tmp/test")
        db.log_download.assert_called_once()
        self.assertEqual(db.log_download.call_args.kwargs["outcome"], "force_import")
        # update_status called by apply_transition
        db.update_status.assert_called_once()

    def test_failure_logs_only(self):
        db = MagicMock()
        outcome = ImportOutcome(success=False, exit_code=5, message="downgrade")
        log_and_update_import(db, 42, outcome,
                              outcome_label="force_import",
                              staged_path="/tmp/test")
        db.log_download.assert_called_once()
        self.assertEqual(db.log_download.call_args.kwargs["outcome"], "failed")
        db.update_status.assert_not_called()


if __name__ == "__main__":
    unittest.main()
