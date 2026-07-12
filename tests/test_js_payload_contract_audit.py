"""Audit JS download fixtures against their real outbound server contracts."""

from __future__ import annotations

import os
import json
import subprocess
import tempfile
import unittest

import msgspec

from tests.structural_audits.js_ast import (
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


def _allowed_fields_by_surface() -> dict[str, set[str]]:
    typed_fields = _server_payload_fields()
    history_fields = {
        field.name for field in msgspec.structs.fields(DownloadHistoryViewRow)
    }
    return {
        "pipeline_log": (
            typed_fields
            | {"in_beets", "beets_format", "beets_bitrate", "beets_avg_bitrate"}
        ),
        "download_history": history_fields,
    }


class TestJsPayloadContractAudit(unittest.TestCase):
    def test_node_confirms_reviewer_renderer_callee_forms_execute(self) -> None:
        script = r'''
const renderDownloadHistoryItem = value => Object.keys(value);
globalThis.renderDownloadHistoryItem = renderDownloadHistoryItem;
const rendererName = "renderDownloadHistoryItem";
const results = [
  renderDownloadHistory\u0049tem({invented_client_only: 1}),
  globalThis["renderDownloadHistoryItem"]({invented_client_only: 1}),
  (0, renderDownloadHistoryItem)({invented_client_only: 1}),
  renderDownloadHistoryItem.call(null, {invented_client_only: 1}),
  renderDownloadHistoryItem?.({invented_client_only: 1}),
  globalThis[rendererName]({invented_client_only: 1}),
];
console.log(JSON.stringify(results));
'''
        result = subprocess.run(
            ["node", "--input-type=module", "--eval", script],
            check=True,
            capture_output=True,
            text=True,
        )
        self.assertEqual(
            json.loads(result.stdout),
            [["invented_client_only"]] * 6,
        )

    def test_node_confirms_dataflow_calls_motivate_explicit_fixture_boundary(
        self,
    ) -> None:
        script = r'''
const helpers = {renderDownloadHistoryItem: value => Object.keys(value)};
const getName = () => "renderDownloadHistoryItem";
const names = ["renderDownloadHistoryItem"];
const lookup = {history: "renderDownloadHistoryItem"};
const prefix = "renderDownloadHistory";
const results = [
  helpers[getName()]({invented_client_only: 1}),
  helpers[names[0]]({invented_client_only: 1}),
  helpers[lookup.history]({invented_client_only: 1}),
  helpers[prefix + "Item"]({invented_client_only: 1}),
  helpers[`${prefix}Item`]({invented_client_only: 1}),
];
console.log(JSON.stringify(results));
'''
        result = subprocess.run(
            ["node", "--input-type=module", "--eval", script],
            check=True,
            capture_output=True,
            text=True,
        )
        self.assertEqual(
            json.loads(result.stdout), [["invented_client_only"]] * 5
        )
        with self.assertRaisesRegex(ValueError, "raw renderer"):
            fixture_fields_for_call(
                "import { renderDownloadHistoryItem as "
                "renderDownloadHistoryFixture } from './fixture.js';\n" + script,
                "renderDownloadHistoryFixture",
                registered_renderer="renderDownloadHistoryItem",
                registered_module="./fixture.js",
            )

    def test_node_confirms_lexical_and_mutable_selectors_execute_renderer(
        self,
    ) -> None:
        script = r'''
const helpers = {renderDownloadHistoryItem: value => Object.keys(value)};
const shadowed = "renderDownloadHistoryItem";
{ const shadowed = "unrelated"; void shadowed; }
const mutated = {history: "unrelated"};
mutated.history = "renderDownloadHistoryItem";
const duplicated = {history: "unrelated", history: "renderDownloadHistoryItem"};
let mutable = "renderDownloadHistoryItem";
const runtime = JSON.parse('"renderDownloadHistoryItem"');
const results = [
  helpers[shadowed]({shadowed: 1}),
  helpers[mutated.history]({mutated: 1}),
  helpers[duplicated.history]({duplicated: 1}),
  helpers[mutable]({mutable: 1}),
  helpers[runtime]({runtime: 1}),
];
console.log(JSON.stringify(results));
'''
        result = subprocess.run(
            ["node", "--input-type=module", "--eval", script],
            check=True,
            capture_output=True,
            text=True,
        )
        self.assertEqual(
            json.loads(result.stdout),
            [["shadowed"], ["mutated"], ["duplicated"], ["mutable"], ["runtime"]],
        )
        with self.assertRaisesRegex(ValueError, "raw renderer"):
            fixture_fields_for_call(
                "import { renderDownloadHistoryItem as "
                "renderDownloadHistoryFixture } from './fixture.js';\n" + script,
                "renderDownloadHistoryFixture",
                registered_renderer="renderDownloadHistoryItem",
                registered_module="./fixture.js",
            )

    def test_explicit_fixture_boundary_rejects_raw_renderer_dataflow(self) -> None:
        registration = "import * as historyModule from './fixture.js'; "
        raw_renderer_uses = (
            (
                'const name = "renderDownloadHistoryItem"; '
                '{ const name = "unrelated"; void name; } '
                "historyModule[name]({invented_client_only: 1});"
            ),
            (
                'const names = {history: "unrelated"}; '
                'names.history = "renderDownloadHistoryItem"; '
                "historyModule[names.history]({invented_client_only: 1});"
            ),
            (
                'const names = {history: "unrelated", '
                'history: "renderDownloadHistoryItem"}; '
                "historyModule[names.history]({invented_client_only: 1});"
            ),
            (
                'let name = "renderDownloadHistoryItem"; '
                "historyModule[name]({invented_client_only: 1});"
            ),
            (
                'const names = {history: "renderDownloadHistoryItem"}; '
                'names.history = "unrelated"; '
                "historyModule[names.history]();"
            ),
        )
        for source in raw_renderer_uses:
            with self.subTest(source=source), self.assertRaisesRegex(
                ValueError, "explicit registration"
            ):
                fixture_fields_for_call(
                    registration + source,
                    "renderDownloadHistoryFixture",
                    registered_renderer="renderDownloadHistoryItem",
                    registered_module="./fixture.js",
                )

    def test_explicit_fixture_registration_audits_only_local_alias_calls(
        self,
    ) -> None:
        source = """
import { renderDownloadHistoryItem as renderDownloadHistoryFixture } from './fixture.js';
console.log('renderDownloadHistoryItem() behavior');
renderDownloadHistoryFixture({outcome: 'success', request_id: 1});
"""
        self.assertEqual(
            fixture_fields_for_call(
                source,
                "renderDownloadHistoryFixture",
                registered_renderer="renderDownloadHistoryItem",
                registered_module="./fixture.js",
            ),
            {"outcome", "request_id"},
        )
        for bypass in (
            "renderDownloadHistoryItem({invented_client_only: 1});",
            "import * as historyModule from './fixture.js'; "
            'const name = "renderDownloadHistoryItem"; '
            "historyModule[name]({invented_client_only: 1});",
            'helpers["renderDownloadHistoryFixture"]({invented_client_only: 1});',
            "__test__.renderDownloadHistoryFixture({invented_client_only: 1});",
        ):
            with self.subTest(bypass=bypass), self.assertRaises(ValueError):
                fixture_fields_for_call(
                    source + bypass,
                    "renderDownloadHistoryFixture",
                    registered_renderer="renderDownloadHistoryItem",
                    registered_module="./fixture.js",
                )

    def test_registration_rejects_mixed_default_import_and_shadowed_test(
        self,
    ) -> None:
        mixed_import = """
import historyDefault, {
  renderDownloadHistoryItem as renderDownloadHistoryFixture,
} from './fixture.js';
renderDownloadHistoryFixture({outcome: 'success'});
"""
        shadowed_test = """
import { __test__ } from './recents.js';
function renderWithShadow(__test__) {
  const { renderRecentsItems: renderRecentsFixture } = __test__;
  return renderRecentsFixture([{outcome: 'success'}]);
}
"""
        cases = (
            (
                mixed_import,
                "renderDownloadHistoryFixture",
                "renderDownloadHistoryItem",
                "./fixture.js",
            ),
            (
                shadowed_test,
                "renderRecentsFixture",
                "renderRecentsItems",
                "./recents.js",
            ),
        )
        for source, fixture, renderer, module in cases:
            with self.subTest(source=source), self.assertRaisesRegex(
                ValueError, "explicit registration"
            ):
                fixture_fields_for_call(
                    source,
                    fixture,
                    registered_renderer=renderer,
                    registered_module=module,
                )

    def test_scanner_decodes_direct_escaped_renderer_identifier(self) -> None:
        self.assertEqual(
            fixture_fields_for_call(
                r"renderDownloadHistory\u0049tem({invented_client_only: 1});",
                "renderDownloadHistoryItem",
            ),
            {"invented_client_only"},
        )

    def test_escaped_identifier_keys_match_node_object_key_semantics(self) -> None:
        object_source = r"{invented\u005fclient: 1, out\u0063ome: 2}"
        result = subprocess.run(
            [
                "node",
                "--input-type=module",
                "--eval",
                f"console.log(JSON.stringify(Object.keys({object_source})));",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        self.assertEqual(
            json.loads(result.stdout), ["invented_client", "outcome"]
        )
        self.assertEqual(
            fixture_fields_for_call(
                f"renderDownloadHistoryItem({object_source});",
                "renderDownloadHistoryItem",
            ),
            {"invented_client", "outcome"},
        )

    def test_scanner_rejects_unsupported_renderer_reference_forms(self) -> None:
        cases = (
            'globalThis["renderDownloadHistoryItem"]({invented_client_only: 1});',
            (
                'const name = "renderDownloadHistoryItem"; '
                "__test__[name]({invented_client_only: 1});"
            ),
            "__test__?.renderDownloadHistoryItem({invented_client_only: 1});",
            "(0, renderDownloadHistoryItem)({invented_client_only: 1});",
            "renderDownloadHistoryItem.call(null, {invented_client_only: 1});",
            "renderDownloadHistoryItem?.({invented_client_only: 1});",
            "const render = renderDownloadHistoryItem; render({invented_client_only: 1});",
            (
                "let render; render = renderDownloadHistoryItem; "
                "render({invented_client_only: 1});"
            ),
            (
                "const {renderDownloadHistoryItem: render} = helpers; "
                "render({invented_client_only: 1});"
            ),
            (
                "let render; ({renderDownloadHistoryItem: render} = helpers); "
                "render({invented_client_only: 1});"
            ),
            (
                "import {renderDownloadHistoryItem as render} from './fixture.js'; "
                "render({invented_client_only: 1});"
            ),
        )
        for source in cases:
            with self.subTest(source=source), self.assertRaisesRegex(
                ValueError, "audited renderer"
            ):
                fixture_fields_for_call(source, "renderDownloadHistoryItem")

    def test_inert_strings_and_unrelated_computed_calls_remain_supported(self) -> None:
        cases = (
            (
                'const key = "unrelated"; globalThis[key]();',
                "renderDownloadHistoryItem",
            ),
            (
                'const label = "renderDownloadHistoryItem"; console.log(label);',
                "renderDownloadHistoryItem",
            ),
            (
                "const key = getRuntimeName(); unrelatedNamespace[key]();",
                "renderDownloadHistoryItem",
            ),
        )
        for source, call_name in cases:
            with self.subTest(source=source):
                self.assertEqual(fixture_fields_for_call(source, call_name), set())

    def test_scanner_uses_utf8_byte_offsets_for_unicode_prefixes(self) -> None:
        source = '''
const decoration = "é🎵";
renderDownloadHistoryItem({outcome: "success", ["caf\\u00e9"]: 1});
'''
        self.assertEqual(
            fixture_fields_for_call(source, "renderDownloadHistoryItem"),
            {"outcome", "café"},
        )

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

    def test_scanner_audits_quoted_property_keys(self) -> None:
        source = """
renderDownloadHistoryItem({"invented_client_only": 1, outcome: "success"});
"""
        fixture_fields = fixture_fields_for_call(
            source, "renderDownloadHistoryItem"
        )
        self.assertEqual(
            fixture_fields,
            {"invented_client_only", "outcome"},
        )
        with self.assertRaisesRegex(AssertionError, "invented_client_only"):
            assert_fixture_fields_have_server_contract(
                {"download_history": fixture_fields},
                {"download_history": {"outcome"}},
            )

    def test_scanner_resolves_static_computed_keys_and_rejects_dynamic_ones(
        self,
    ) -> None:
        self.assertEqual(
            fixture_fields_for_call(
                'renderDownloadHistoryItem({["static_key"]: 1});',
                "renderDownloadHistoryItem",
            ),
            {"static_key"},
        )
        with self.assertRaisesRegex(ValueError, "computed"):
            fixture_fields_for_call(
                "renderDownloadHistoryItem({[fieldName]: 1});",
                "renderDownloadHistoryItem",
            )

    def test_scanner_audits_calls_inside_template_interpolation(self) -> None:
        source = """
const markup = `before ${renderDownloadHistoryItem({
  invented_in_template: 1,
  outcome: "success",
})} after`;
"""
        self.assertEqual(
            fixture_fields_for_call(source, "renderDownloadHistoryItem"),
            {"invented_in_template", "outcome"},
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

    def test_scanner_fails_closed_on_syntax_errors(self) -> None:
        with self.assertRaisesRegex(ValueError, "JavaScript parse error"):
            fixture_fields_for_call(
                "renderDownloadHistoryItem({outcome: );",
                "renderDownloadHistoryItem",
            )

    def test_scanner_reaches_every_js_test_module(self) -> None:
        with tempfile.TemporaryDirectory() as tests_dir:
            with open(
                os.path.join(tests_dir, "test_js_future.mjs"),
                "w",
                encoding="utf-8",
            ) as handle:
                handle.write(
                    "import { renderEvidenceStrip as renderEvidenceFixture } "
                    "from '../web/js/history.js';\n"
                    "renderEvidenceFixture({ future_field: 1 });\n"
                )
            scanned = scan_js_payload_fixture_fields(tests_dir)
        self.assertEqual(scanned["download_history"], {"future_field"})

    def test_corpus_registration_is_optional_only_without_boundary_references(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tests_dir:
            path = os.path.join(tests_dir, "test_js_unrelated.mjs")
            with open(path, "w", encoding="utf-8") as handle:
                handle.write(
                    "const value = unrelatedNamespace[key]();\n"
                    "console.log('renderDownloadHistoryItem is only prose');\n"
                )
            self.assertEqual(
                scan_js_payload_fixture_fields(tests_dir),
                {"pipeline_log": set(), "download_history": set()},
            )
            with open(path, "w", encoding="utf-8") as handle:
                handle.write(
                    "import { unrelated } from '../web/js/history.js';\n"
                    "console.log('renderDownloadHistoryItem is still prose');\n"
                )
            self.assertEqual(
                scan_js_payload_fixture_fields(tests_dir),
                {"pipeline_log": set(), "download_history": set()},
            )
            with open(path, "w", encoding="utf-8") as handle:
                handle.write(
                    "import * as historyModule from '../web/js/history.js';\n"
                    'const name = "renderDownloadHistoryItem"; '
                    "historyModule[name]({ invented_client_only: 1 });\n"
                )
            with self.assertRaisesRegex(ValueError, "explicit registration"):
                scan_js_payload_fixture_fields(tests_dir)
            with open(path, "w", encoding="utf-8") as handle:
                handle.write(
                    "import historyModule from '../web/js/history.js';\n"
                    "historyModule[selector]({ invented_client_only: 1 });\n"
                )
            with self.assertRaisesRegex(ValueError, "explicit registration"):
                scan_js_payload_fixture_fields(tests_dir)

    def test_corpus_parse_error_names_the_scanned_js_module(self) -> None:
        with tempfile.TemporaryDirectory() as tests_dir:
            path = os.path.join(tests_dir, "test_js_future.mjs")
            with open(path, "w", encoding="utf-8") as handle:
                handle.write("const broken = ;\n")
            with self.assertRaisesRegex(
                ValueError,
                r"test_js_future\.mjs: JavaScript parse error",
            ):
                scan_js_payload_fixture_fields(tests_dir)

    def test_corpus_semantic_error_names_the_scanned_js_module(self) -> None:
        with tempfile.TemporaryDirectory() as tests_dir:
            path = os.path.join(tests_dir, "test_js_bad_boundary.mjs")
            with open(path, "w", encoding="utf-8") as handle:
                handle.write(
                    "import * as historyModule from '../web/js/history.js';\n"
                    "historyModule[selector]({ invented_client_only: 1 });\n"
                )
            with self.assertRaisesRegex(
                ValueError,
                r"test_js_bad_boundary\.mjs: .*explicit registration",
            ):
                scan_js_payload_fixture_fields(tests_dir)

    def test_every_seeded_download_field_has_a_server_contract(self) -> None:
        assert_fixture_fields_have_server_contract(
            scan_js_payload_fixture_fields(),
            _allowed_fields_by_surface(),
        )

    def test_download_history_rejects_log_only_transfer_detail(self) -> None:
        fixture_fields = fixture_fields_for_call(
            "renderDownloadHistoryItem({ transfer_detail: [] });",
            "renderDownloadHistoryItem",
        )
        with self.assertRaisesRegex(AssertionError, "transfer_detail"):
            assert_fixture_fields_have_server_contract(
                {"download_history": fixture_fields},
                _allowed_fields_by_surface(),
            )

    def test_checker_rejects_a_client_only_seeded_field(self) -> None:
        with self.assertRaisesRegex(AssertionError, "invented_client_only"):
            assert_fixture_fields_have_server_contract(
                {"pipeline_log": {"outcome", "invented_client_only"}},
                {"pipeline_log": {"outcome"}},
            )


if __name__ == "__main__":
    unittest.main()
