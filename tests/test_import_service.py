"""Tests for lib/import_service.py — ImportResult extraction helpers."""

import json
import unittest

from lib.import_service import (
    parse_import_result_stdout,
    extract_import_update_fields,
    extract_import_log_fields,
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
        self.assertEqual(fields["last_download_spectral_grade"], "genuine")
        self.assertEqual(fields["last_download_spectral_bitrate"], 256)
        self.assertEqual(fields["current_spectral_grade"], "genuine")
        self.assertEqual(fields["current_spectral_bitrate"], 245)
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
        self.assertEqual(fields["last_download_spectral_grade"], "genuine")
        self.assertEqual(fields["last_download_spectral_bitrate"], 256)
        self.assertEqual(fields["current_spectral_grade"], "genuine")
        self.assertEqual(fields["current_spectral_bitrate"], 245)
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

    def test_wrong_typed_field_returns_empty(self):
        """Issue #141 regression: strict ``msgspec`` decode now raises
        ``msgspec.ValidationError`` on type drift in a JSONB row, not the
        legacy ``TypeError``/``KeyError``. ``extract_import_log_fields``
        must still degrade to ``{}`` — historical download_log rows may
        carry stale shapes and the backfill path treats "can't extract"
        as "leave denormalised columns null", not "500 the caller".
        """
        import json as _json
        malformed = _json.dumps({
            "version": 2,
            "exit_code": "not-an-int",  # declared int → ValidationError
            "decision": "import",
        })
        self.assertEqual(extract_import_log_fields(malformed), {})


if __name__ == "__main__":
    unittest.main()
