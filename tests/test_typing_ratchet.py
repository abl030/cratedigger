"""Audit: production typing escape hatches only ever decrease (issue #765).

Production code is migrating to pyright strict; explicit ``Any``,
``cast(...)``, ``# type: ignore``, and bare ``# pyright: ignore`` are banned.
Existing debt is held in ``tests/_typing_ratchet_baseline.py`` and must
match the live scan EXACTLY: new escape hatches fail, and improvements must
shrink the baseline in the same PR.
"""

from __future__ import annotations

import os
import unittest

from tests._typing_ratchet_baseline import TYPING_RATCHET_BASELINE
from tests._typing_ratchet_scanner import (
    count_escape_hatches,
    iter_production_paths,
    scan_production_tree,
)

_REGEN = (
    'nix-shell --run "python3 -m tests._typing_ratchet_scanner"'
    " > tests/_typing_ratchet_baseline.py"
)


class TestTypingEscapeHatchRatchet(unittest.TestCase):
    """Live production counts must equal the baseline exactly."""

    def test_production_counts_match_baseline_exactly(self) -> None:
        live = scan_production_tree()
        if live == TYPING_RATCHET_BASELINE:
            return
        regressions: list[str] = []
        improvements: list[str] = []
        for rel in sorted(set(live) | set(TYPING_RATCHET_BASELINE)):
            live_counts = live.get(rel, {})
            base_counts = TYPING_RATCHET_BASELINE.get(rel, {})
            for key in sorted(set(live_counts) | set(base_counts)):
                n_live = live_counts.get(key, 0)
                n_base = base_counts.get(key, 0)
                if n_live > n_base:
                    regressions.append(f"{rel}: {key} {n_base} -> {n_live}")
                elif n_live < n_base:
                    improvements.append(f"{rel}: {key} {n_base} -> {n_live}")
        msg = ["Typing escape-hatch ratchet mismatch (issue #765)."]
        if regressions:
            msg.append(
                "NEW escape hatches in production code — remove them "
                "(explicit Any, cast(), # type: ignore, and bare "
                "# pyright: ignore are banned; scoped "
                "# pyright: ignore[rule] is the only sanctioned form):\n  "
                + "\n  ".join(regressions)
            )
        if improvements:
            msg.append(
                "Escape hatches removed — tighten the baseline in this "
                f"same PR:\n  {_REGEN}\nImproved:\n  "
                + "\n  ".join(improvements)
            )
        self.fail("\n".join(msg))


class TestScannerTripsOnViolations(unittest.TestCase):
    """Known-bad self-tests: each banned pattern must be counted."""

    def test_explicit_any_annotation_counts(self) -> None:
        src = "from typing import Any\n\ndef f(x: Any) -> Any:\n    return x\n"
        self.assertEqual(count_escape_hatches(src)["any"], 3)

    def test_cast_call_counts(self) -> None:
        src = "from typing import cast\n\nvalue = cast(int, raw)\n"
        self.assertEqual(count_escape_hatches(src)["cast"], 2)

    def test_attribute_access_any_counts(self) -> None:
        src = "import typing\n\ndef f(x: typing.Any) -> None: ...\n"
        self.assertEqual(count_escape_hatches(src)["any"], 1)

    def test_aliased_any_import_still_counts(self) -> None:
        src = "from typing import Any as _A\n\ndef f(x: _A) -> None: ...\n"
        self.assertEqual(count_escape_hatches(src)["any"], 1)

    def test_type_ignore_comment_counts(self) -> None:
        cases = [
            ("bare", "import foo  # type: ignore\n"),
            ("coded", "import foo  # type: ignore[import-untyped]\n"),
            ("spaced", "import foo  #type:ignore\n"),
            ("own line", "# type: ignore\nimport foo\n"),
        ]
        for desc, src in cases:
            with self.subTest(desc=desc):
                self.assertEqual(count_escape_hatches(src)["type_ignore"], 1)

    def test_bare_pyright_ignore_counts(self) -> None:
        src = "x = f()  # pyright: ignore\n"
        self.assertEqual(count_escape_hatches(src)["bare_pyright_ignore"], 1)

    def test_combined_comment_counts_each_marker(self) -> None:
        src = "import m  # type: ignore[attr-defined]  # pyright: ignore\n"
        counts = count_escape_hatches(src)
        self.assertEqual(counts["type_ignore"], 1)
        self.assertEqual(counts["bare_pyright_ignore"], 1)


class TestScannerDoesNotOvercount(unittest.TestCase):
    """Sanctioned or incidental mentions must NOT be counted."""

    def test_scoped_pyright_ignore_is_sanctioned(self) -> None:
        src = "x = f()  # pyright: ignore[reportArgumentType]\n"
        self.assertEqual(count_escape_hatches(src), {})

    def test_strings_docstrings_comments_do_not_count_names(self) -> None:
        src = (
            '"""Any cast( of Any in a docstring."""\n'
            'msg = "Any cast("\n'
            "# Any cast( in a comment\n"
        )
        self.assertEqual(count_escape_hatches(src), {})

    def test_type_ignored_prose_does_not_count(self) -> None:
        src = "# this type: ignored the hint\n"
        self.assertEqual(count_escape_hatches(src), {})

    def test_clean_source_counts_nothing(self) -> None:
        src = "def f(x: int) -> int:\n    return x\n"
        self.assertEqual(count_escape_hatches(src), {})


class TestProductionWalk(unittest.TestCase):
    """Pin the walker's coverage and its exclusions."""

    def test_walk_reaches_every_production_surface(self) -> None:
        rels = {rel for rel, _ in iter_production_paths()}
        for expected in (
            "cratedigger.py",
            "album_source.py",
            os.path.join("lib", "search.py"),
            os.path.join("web", "server.py"),
            os.path.join("scripts", "pipeline_cli", "cli.py"),
            os.path.join("harness", "import_one.py"),
            os.path.join("tools", "generate-ai-adapters.py"),
        ):
            self.assertIn(expected, rels)

    def test_walk_excludes_tests_hidden_dirs_and_vulture(self) -> None:
        rels = {rel for rel, _ in iter_production_paths()}
        for rel in rels:
            parts = rel.split(os.sep)
            for pruned in ("tests", "docs", "examples", "build",
                           "__pycache__"):
                self.assertNotIn(pruned, parts[:-1], rel)
            self.assertFalse(
                any(p.startswith(".") for p in parts), rel)
            self.assertFalse(
                rel.startswith(os.path.join("tools", "vulture")), rel)


if __name__ == "__main__":
    unittest.main()
