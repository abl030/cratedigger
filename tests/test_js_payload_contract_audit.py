"""Audit JS download fixtures against their real outbound server contracts."""

from __future__ import annotations

import os
import tempfile
import unittest

import msgspec

from tests._js_payload_contract_scanner import (
    assert_fixture_fields_have_server_contract,
    fixture_fields_for_call,
    scan_js_payload_fixture_fields,
)
from web.classify import ClassifiedEntry, LogEntry
from web.download_history_view import DownloadHistoryViewRow


def _server_payload_fields() -> set[str]:
    return {field.name for field in msgspec.structs.fields(ClassifiedEntry)} | set(
        LogEntry.__annotations__
    )


class TestJsPayloadContractAudit(unittest.TestCase):
    def test_scanner_extracts_direct_fields_without_nested_payload_keys(self) -> None:
        source = """
const outcome = 'success';
console.log('renderDownloadHistoryItem({ fake_string_field: 1 })');
renderDownloadHistoryItem({
  outcome,
  comparison_basis: { verdict: 'better', branch: 'rank' },
  bad_extensions: ['one.bak'],
});
"""
        self.assertEqual(
            fixture_fields_for_call(source, "renderDownloadHistoryItem"),
            {"outcome", "comparison_basis", "bad_extensions"},
        )

    def test_scanner_rejects_spreads_and_indirect_fixtures(self) -> None:
        cases = (
            "renderDownloadHistoryItem({ ...base, outcome: 'success' });",
            "renderDownloadHistoryItem(fixture);",
            "renderRecentsItems([...rows]);",
            "renderRecentsItems([fixture]);",
        )
        for source in cases:
            call_name = (
                "renderRecentsItems"
                if "renderRecentsItems" in source
                else "renderDownloadHistoryItem"
            )
            with self.subTest(source=source), self.assertRaises(ValueError):
                fixture_fields_for_call(source, call_name)

    def test_scanner_reaches_every_js_test_module(self) -> None:
        with tempfile.TemporaryDirectory() as tests_dir:
            with open(
                os.path.join(tests_dir, "test_js_future.mjs"),
                "w",
                encoding="utf-8",
            ) as handle:
                handle.write("renderEvidenceStrip({ future_field: 1 });\n")
            scanned = scan_js_payload_fixture_fields(tests_dir)
        self.assertEqual(scanned["download_history"], {"future_field"})

    def test_every_seeded_download_field_has_a_server_contract(self) -> None:
        typed_fields = _server_payload_fields()
        history_fields = {
            field.name for field in msgspec.structs.fields(DownloadHistoryViewRow)
        }
        assert_fixture_fields_have_server_contract(
            scan_js_payload_fixture_fields(),
            {
                "pipeline_log": (
                    typed_fields
                    | {"in_beets", "beets_format", "beets_bitrate"}
                ),
                "download_history": typed_fields | history_fields,
            },
        )

    def test_checker_rejects_a_client_only_seeded_field(self) -> None:
        with self.assertRaisesRegex(AssertionError, "invented_client_only"):
            assert_fixture_fields_have_server_contract(
                {"pipeline_log": {"outcome", "invented_client_only"}},
                {"pipeline_log": {"outcome"}},
            )


if __name__ == "__main__":
    unittest.main()
