"""Qualification pins for the shared tree-sitter JavaScript foundation."""

from __future__ import annotations

import glob
import os
import unittest

from tests.structural_audits.js_ast import parse_javascript


REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class TestJavascriptAstFoundation(unittest.TestCase):
    def test_valid_unrelated_javascript_is_not_rejected_by_a_lexical_subset(
        self,
    ) -> None:
        source = r'''
const matcher = /[`{}a-z\/]+/giu;
class Example { #value = matcher; method() { return this?.#value ?? /x/.source; } }
const markup = `before ${new Example().method()} after`;
export { Example, markup };
'''
        tree = parse_javascript(source, origin="unrelated-syntax.mjs")
        self.assertEqual(tree.root_node.type, "program")

    def test_parser_fails_closed_with_the_origin_on_error(self) -> None:
        with self.assertRaisesRegex(
            ValueError, r"broken\.mjs: JavaScript parse error"
        ):
            parse_javascript("const broken = ;", origin="broken.mjs")

    def test_every_audited_js_module_and_web_source_parses(self) -> None:
        paths = sorted(
            glob.glob(os.path.join(REPO_ROOT, "tests", "test_js_*.mjs"))
            + glob.glob(os.path.join(REPO_ROOT, "web", "js", "*.js"))
        )
        self.assertTrue(paths)
        for path in paths:
            with self.subTest(path=os.path.relpath(path, REPO_ROOT)):
                with open(path, "r", encoding="utf-8") as handle:
                    parse_javascript(
                        handle.read(), origin=os.path.relpath(path, REPO_ROOT)
                    )


if __name__ == "__main__":
    unittest.main()
