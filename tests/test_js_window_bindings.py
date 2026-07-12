"""Conservative AST-backed audit for statically authored window handlers."""

from __future__ import annotations

import os
import json
from pathlib import Path
import subprocess
import unittest

from tests.structural_audits.js_ast import (
    assert_window_bindings,
    audit_window_bindings,
    emitted_window_handlers,
    exposed_window_bindings,
)


TEST_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(TEST_DIR)
WEB_JS_DIR = os.path.join(REPO_ROOT, "web", "js")


def _web_sources() -> dict[str, str]:
    return {
        name: Path(WEB_JS_DIR, name).read_text(encoding="utf-8")
        for name in sorted(os.listdir(WEB_JS_DIR))
        if name.endswith(".js")
    }


class TestJsWindowBindings(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.js_sources = _web_sources()
        with open(
            os.path.join(REPO_ROOT, "web", "index.html"), encoding="utf-8"
        ) as handle:
            cls.index_html = handle.read()
        cls.main_source = cls.js_sources["main.js"]

    def test_alias_uses_the_public_key_and_nested_values_stay_private(self) -> None:
        release_handlers = emitted_window_handlers(
            {"release_actions.js": self.js_sources["release_actions.js"]}, ""
        )
        self.assertIn("openReplacePicker", release_handlers.handlers)
        real_bindings = exposed_window_bindings(self.main_source)
        self.assertIn("openReplacePicker", real_bindings)
        self.assertNotIn("openReplacePickerAndHandle", real_bindings)

        self.assertEqual(
            exposed_window_bindings(
                """Object.assign(window, {
  shorthand,
  publicAlias: localImplementation,
  namespace: { nestedHandler, deeper: { hiddenHandler } },
});"""
            ),
            {"namespace", "publicAlias", "shorthand"},
        )

    def test_multiple_direct_binding_blocks_are_unioned(self) -> None:
        self.assertEqual(
            exposed_window_bindings(
                "Object.assign(window, { first }); "
                "Object.assign(window, { alias: second });"
            ),
            {"first", "alias"},
        )

    def test_dynamic_handler_bodies_are_allowed_when_static_names_exist(self) -> None:
        discovery = emitted_window_handlers(
            {
                name: self.js_sources[name]
                for name in (
                    "pipeline.js",
                    "render_primitives.js",
                    "discography.js",
                )
            },
            "",
        )
        for name in (
            "loadLongTail",
            "loadPipelineDashboard",
            "loadPipeline",
            "toggleReleaseDetail",
        ):
            self.assertIn(name, discovery.handlers)
        self.assertEqual(discovery.dynamic_callees, ())

        assert_window_bindings(
            {"body.js": 'const html = `<button onclick="${handlerBody}">x</button>`;'},
            "",
            "Object.assign(window, {});",
        )

    def test_only_literal_surfaces_and_index_onclick_handlers_count(self) -> None:
        discovery = emitted_window_handlers(
            {
                "fixture.js": """
// const ignored = '<button onclick="window.commentOnly()">';
window.ordinaryCodeOnly();
const help = 'Debug with window.helpStringHandler()';
const native = 'window.fetch("/api")';
"""
            },
            '<button onclick="bareIndexHandler()">x</button>',
        )
        self.assertEqual(
            discovery.handlers, {"bareIndexHandler", "helpStringHandler"}
        )

    def test_static_handler_names_encoded_with_js_escapes_are_cooked(self) -> None:
        discovery = emitted_window_handlers(
            {
                "escaped.js": r'''
const quoted = 'window.\u0066oo()';
const templated = `window.\x62ar()`;
'''
            },
            "",
        )
        self.assertEqual(discovery.handlers, {"foo", "bar"})

    def test_ecmascript_unicode_line_continuations_are_cooked(self) -> None:
        for separator in ("\u2028", "\u2029"):
            for delimiter in ("'", "`"):
                literal = (
                    f"{delimiter}window.hidden\\{separator}Handler()"
                    f"{delimiter}"
                )
                script = (
                    f"const value = {literal}; "
                    "console.log(JSON.stringify(value));"
                )
                result = subprocess.run(
                    ["node", "--input-type=module", "--eval", script],
                    check=True,
                    capture_output=True,
                    text=True,
                )
                self.assertEqual(
                    json.loads(result.stdout), "window.hiddenHandler()"
                )
                discovery = emitted_window_handlers(
                    {"continuation.js": f"const value = {literal};"}, ""
                )
                self.assertEqual(discovery.handlers, {"hiddenHandler"})

    def test_valid_lone_and_braced_surrogate_escapes_are_not_rejected(self) -> None:
        source = r'''
const values = ['\uD800', '\uDC00', '\u{D800}', '\u{DC00}', `\uD800`, `\u{D800}`];
console.log(JSON.stringify(values.map(value => value.length)));
'''
        result = subprocess.run(
            ["node", "--input-type=module", "--eval", source],
            check=True,
            capture_output=True,
            text=True,
        )
        self.assertEqual(json.loads(result.stdout), [1, 1, 1, 1, 1, 1])
        self.assertEqual(
            emitted_window_handlers({"surrogates.js": source}, "").handlers,
            set(),
        )

    def test_missing_static_handler_known_bad_trips(self) -> None:
        js_sources = {
            "bad.js": "const html = '<button onclick=\"window.unboundStaticHandler()\">x</button>';"
        }
        audit = audit_window_bindings(
            js_sources, "", "Object.assign(window, {});"
        )
        self.assertEqual(audit.missing, {"unboundStaticHandler"})
        with self.assertRaisesRegex(ValueError, "unboundStaticHandler"):
            assert_window_bindings(js_sources, "", "Object.assign(window, {});")

    def test_dynamic_and_computed_callees_fail_closed(self) -> None:
        cases = (
            'const html = `<button onclick="window.${handlerName}()">x</button>`;',
            r'const html = `<button onclick="window\u002e${handlerName}()">x</button>`;',
            "const html = '<button onclick=\"window[handlerName]()\">x</button>';",
        )
        for source in cases:
            with self.subTest(source=source):
                audit = audit_window_bindings(
                    {"dynamic.js": source}, "", "Object.assign(window, {});"
                )
                self.assertTrue(audit.dynamic_callees)
                with self.assertRaisesRegex(ValueError, "dynamic window callee"):
                    assert_window_bindings(
                        {"dynamic.js": source},
                        "",
                        "Object.assign(window, {});",
                    )

    def test_native_calls_are_filtered_and_binding_collisions_rejected(self) -> None:
        assert_window_bindings(
            {
                "native.js": (
                    "const help = 'window.fetch(\"/api\"); "
                    "window.open(\"/\")';"
                )
            },
            "",
            "Object.assign(window, {});",
        )
        audit = audit_window_bindings(
            {}, "", "Object.assign(window, { fetch });"
        )
        self.assertEqual(audit.native_collisions, {"fetch"})
        with self.assertRaisesRegex(ValueError, "reserved native window names: fetch"):
            assert_window_bindings({}, "", "Object.assign(window, { fetch });")

    def test_unsupported_binding_forms_and_parser_errors_fail_closed(self) -> None:
        for source in (
            "Object.assign(window, { ...bindings });",
            'Object.assign(window, { [name]: handler });',
            'Object.assign(window, { "quoted": handler });',
            "Object.assign(window, { direct }, extraBindings);",
            (
                "Object.assign(window, { supported }); "
                "Object['assign'](window, { fetch });"
            ),
            (
                "Object.assign(window, { supported }); "
                "Object.assign((window), { fetch });"
            ),
            (
                "Object.assign(window, { supported }); "
                r"Object.\u0061ssign(window, { fetch });"
            ),
            (
                "Object.assign(window, { supported }); "
                "Object.assign?.(window, { fetch });"
            ),
            (
                'const method = "assign"; '
                "Object.assign(window, { supported }); "
                "Object[method](window, { fetch });"
            ),
            "Object.assign(window, { supported }); Object.assign(globalThis, { fetch });",
            "Object.assign(window, { supported }); Object.assign(self, { fetch });",
            "Object.assign(window, { supported }); globalThis.fetch = localFetch;",
            "Object.assign(window, { supported }); self['fetch'] = localFetch;",
            "Object.assign(window, { supported }); Object?.assign(window, { fetch });",
            (
                "const assignWindow = Object.assign; "
                "Object.assign(window, { supported }); "
                "assignWindow(window, { fetch });"
            ),
            (
                "const {assign: assignWindow} = Object; "
                "Object.assign(window, { supported }); "
                "assignWindow(window, { fetch });"
            ),
            (
                "let assignWindow; assignWindow = Object.assign; "
                "Object.assign(window, { supported }); "
                "assignWindow(window, { fetch });"
            ),
            (
                "const targetWindow = window; "
                "Object.assign(window, { supported }); "
                "Object.assign(targetWindow, { fetch });"
            ),
            "Object.assign(window, { supported }); (window).fetch = localFetch;",
            "Object.assign(window, { broken: });",
        ):
            with self.subTest(source=source), self.assertRaises(ValueError):
                exposed_window_bindings(source)

    def test_unrelated_computed_object_call_does_not_trip_binding_audit(self) -> None:
        source = (
            'const method = "unrelated"; '
            "Object.assign(window, { supported }); "
            "Object[method](window, { fetch });"
        )
        self.assertEqual(exposed_window_bindings(source), {"supported"})

    def test_escaped_binding_key_is_normalized_before_native_collision(self) -> None:
        source = r"Object.assign(window, { f\u0065tch });"
        self.assertEqual(exposed_window_bindings(source), {"fetch"})
        audit = audit_window_bindings({}, "", source)
        self.assertEqual(audit.native_collisions, {"fetch"})
        with self.assertRaisesRegex(ValueError, "reserved native window names: fetch"):
            assert_window_bindings({}, "", source)

    def test_node_confirms_dynamic_assign_and_global_mutations_execute(self) -> None:
        script = r'''
const target = {};
const f\u0065tch = 1;
const method = "assign";
Object[method](target, {f\u0065tch});
globalThis.reviewProbe = 2;
console.log(JSON.stringify([Object.keys(target), globalThis.reviewProbe]));
delete globalThis.reviewProbe;
'''
        result = subprocess.run(
            ["node", "--input-type=module", "--eval", script],
            check=True,
            capture_output=True,
            text=True,
        )
        self.assertEqual(json.loads(result.stdout), [["fetch"], 2])

    def test_production_corpus_has_every_conservative_handler_bound(self) -> None:
        audit = assert_window_bindings(
            self.js_sources, self.index_html, self.main_source
        )
        self.assertTrue(audit.required)


if __name__ == "__main__":
    unittest.main()
