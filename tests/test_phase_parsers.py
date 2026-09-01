"""One phase's log in, its failure index out — per dialect (issue #1313).

`scripts/phase_parsers/` replaced the coordinator's six-way branch on a
stringly `PhaseSpec.parser` tag with one callable per wrapper. These are
the dialect tests: they hand a parser the text a real tool writes and
assert what lands in the bundle's index, with no suite, no subprocess,
and no bundle in the way.

Deterministic only. Test infrastructure never becomes a generated-test
subject (`.claude/rules/code-quality.md` § "Testing — Red/Green TDD").
"""

from __future__ import annotations

import os
import sys
import unittest

import msgspec

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.phase_parsers import (
    CheckFailure,
    PhaseFailures,
    PhaseLog,
    dead_code,
    js_checks,
    pyright_checks,
    python_tests,
    ruff,
)


def _log(text: str, *, rerun: str = "bash scripts/rerun.sh") -> PhaseLog:
    return PhaseLog(text=text, log_name="phase.log", rerun_command=rerun)


class TestJavaScriptSyntaxDialect(unittest.TestCase):
    """`run_js_checks.sh syntax` — the identity is the unparseable file."""

    def test_a_marker_becomes_one_entry_rerunnable_with_node_check(
        self,
    ) -> None:
        (failure,) = js_checks.parse_syntax_failures(
            _log("CRATEDIGGER_JS_FAILURE\tweb/js/bad.js\tnode --check failed")
        ).failures

        self.assertEqual(failure.identity, "web/js/bad.js")
        self.assertEqual(failure.owner, "web/js/bad.js")
        self.assertEqual(failure.detail, "node --check failed")
        self.assertEqual(
            failure.rerun_command,
            "node --check --input-type=module < web/js/bad.js",
        )
        self.assertEqual(failure.log, "phase.log")

    def test_a_path_needing_quoting_is_quoted_in_the_rerun(self) -> None:
        (failure,) = js_checks.parse_syntax_failures(
            _log("CRATEDIGGER_JS_FAILURE\tweb/js/od d.js\tSyntaxError")
        ).failures

        self.assertEqual(
            failure.rerun_command,
            "node --check --input-type=module < 'web/js/od d.js'",
        )

    def test_lines_that_are_not_markers_are_ignored(self) -> None:
        parsed = js_checks.parse_syntax_failures(
            _log(
                "checking web/js/a.js\n"
                "CRATEDIGGER_JS_FAILURE_OTHER\tweb/js/a.js\tnope\n"
                " CRATEDIGGER_JS_FAILURE\tweb/js/b.js\tindented"
            )
        )

        self.assertEqual(parsed, PhaseFailures())

    def test_a_non_marker_line_is_skipped_not_treated_as_the_end(self) -> None:
        """The wrapper interleaves `node --check` chatter with its markers.

        Only a log where an ignored line comes BEFORE a real marker tells
        `continue` apart from `break`; the mutmut catalog found the
        earlier version of the test above could not (survivor
        `js_checks.x__records__mutmut_4`).
        """
        parsed = js_checks.parse_syntax_failures(
            _log(
                "checking web/js/a.js\n"
                "CRATEDIGGER_JS_FAILURE\tweb/js/b.js\tSyntaxError"
            )
        )

        self.assertEqual(
            [failure.identity for failure in parsed.failures], ["web/js/b.js"]
        )


class TestJavaScriptUnitDialect(unittest.TestCase):
    """A per-assertion identity still yields a runnable per-FILE rerun.

    `tests/js_harness.mjs` names one failed assertion per marker, as
    `<file>::<section>::<message>`, so the owner and the rerun come from
    the file half: the index entry stays specific while `node <owner>`
    stays something you can actually paste. Moved here verbatim in
    substance from `tests/test_suite_coordinator.py`, which used to reach
    the same code through `PhaseSpec` and `_parse_failures`.
    """

    def _failures(self, marker: str) -> tuple[CheckFailure, ...]:
        return js_checks.parse_unit_failures(_log(marker)).failures

    def test_owner_and_rerun_come_from_the_file_half_of_the_identity(
        self,
    ) -> None:
        (failure,) = self._failures(
            "CRATEDIGGER_JS_FAILURE\t"
            "tests/test_js_recents.mjs::renderRecents()::badge is escaped\t"
            "expected 'a', got 'b'",
        )

        self.assertEqual(
            failure.identity,
            "tests/test_js_recents.mjs::renderRecents()::badge is escaped",
        )
        self.assertEqual(failure.owner, "tests/test_js_recents.mjs")
        self.assertEqual(
            failure.rerun_command, "node tests/test_js_recents.mjs"
        )
        self.assertEqual(failure.detail, "expected 'a', got 'b'")

    def test_two_assertions_in_one_file_are_two_distinct_index_entries(
        self,
    ) -> None:
        first, second = self._failures(
            "CRATEDIGGER_JS_FAILURE\ttests/test_js_a.mjs::s::one\tboom\n"
            "CRATEDIGGER_JS_FAILURE\ttests/test_js_a.mjs::s::two\tbang",
        )

        self.assertNotEqual(first.identity, second.identity)
        self.assertEqual(first.owner, second.owner)
        self.assertEqual(first.rerun_command, second.rerun_command)

    def test_a_marker_with_too_few_fields_is_refused_not_guessed(self) -> None:
        """A short marker is a broken tool, not a failure to half-read.

        The parser raises rather than unpacking whatever it got, and the
        coordinator turns that into an infrastructure-failure, the honest
        label. Nothing constrained either the refusal or the split's field
        bound before (issue #1319 independent review, mutants C1 and C2).
        """
        with self.assertRaises(ValueError) as caught:
            self._failures("CRATEDIGGER_JS_FAILURE\tonly-two-fields")
        self.assertIn(
            "malformed JavaScript failure marker", str(caught.exception)
        )

    def test_a_short_marker_is_refused_by_the_syntax_dialect_too(
        self,
    ) -> None:
        """Both modes share one record reader, so both refuse identically."""
        with self.assertRaises(ValueError):
            js_checks.parse_syntax_failures(
                _log("CRATEDIGGER_JS_FAILURE\tonly-two-fields")
            )

    def test_a_tab_inside_the_detail_stays_in_the_detail(self) -> None:
        """The split stops at three fields, so field three keeps its tabs.

        The harness collapses tabs before writing, but the bound is what
        makes a fourth field impossible: widening it would drop everything
        after a stray tab instead of keeping it as detail.
        """
        (failure,) = self._failures(
            "CRATEDIGGER_JS_FAILURE\ttests/test_js_a.mjs::s::one\t"
            "left\tright",
        )

        self.assertEqual(failure.identity, "tests/test_js_a.mjs::s::one")
        self.assertEqual(failure.detail, "left\tright")

    def test_an_identity_with_no_separator_is_its_own_owner(self) -> None:
        """A file-level marker from the wrapper, not the harness."""
        (failure,) = self._failures(
            "CRATEDIGGER_JS_FAILURE\ttests/test_js_a.mjs\t"
            "suite exited before reaching checker.done()",
        )

        self.assertEqual(failure.owner, "tests/test_js_a.mjs")
        self.assertEqual(failure.rerun_command, "node tests/test_js_a.mjs")


class TestPyrightDialect(unittest.TestCase):
    """`run_pyright_checks.py` — position is the identity."""

    def test_an_error_becomes_one_entry_at_its_position(self) -> None:
        (failure,) = pyright_checks.parse_failures(
            _log(
                "lib/typed.py:7:4 - error: Argument is unknown "
                "(reportUnknownArgumentType)",
                rerun="python3 scripts/run_pyright_checks.py",
            )
        ).failures

        self.assertEqual(failure.identity, "lib/typed.py:7:4")
        self.assertEqual(failure.owner, "lib/typed.py")
        self.assertEqual(
            failure.detail,
            "Argument is unknown (reportUnknownArgumentType)",
        )
        self.assertEqual(
            failure.rerun_command, "python3 scripts/run_pyright_checks.py"
        )

    def test_warnings_and_informations_are_not_indexed(self) -> None:
        """Only errors fail the phase, so only errors earn an entry."""
        parsed = pyright_checks.parse_failures(
            _log(
                "lib/a.py:1:1 - warning: Unused import\n"
                "lib/a.py:2:1 - information: Type of x is int\n"
                "0 errors, 1 warning, 1 information"
            )
        )

        self.assertEqual(parsed.failures, ())

    def test_every_error_line_gets_its_own_entry(self) -> None:
        parsed = pyright_checks.parse_failures(
            _log(
                "lib/a.py:1:1 - error: first\n"
                "noise in between\n"
                "lib/b.py:2:3 - error: second"
            )
        )

        self.assertEqual(
            [failure.identity for failure in parsed.failures],
            ["lib/a.py:1:1", "lib/b.py:2:3"],
        )


class TestRuffDialect(unittest.TestCase):
    """`run_ruff.sh` — two output formats, one of them stateful."""

    def test_full_format_pairs_a_header_with_the_location_below_it(
        self,
    ) -> None:
        (failure,) = ruff.parse_failures(
            _log("F821 Undefined name `missing`\n --> lib/lint.py:9:2")
        ).failures

        self.assertEqual(failure.identity, "lib/lint.py:9:2")
        self.assertEqual(failure.owner, "lib/lint.py")
        self.assertEqual(failure.detail, "F821 Undefined name `missing`")
        self.assertEqual(
            failure.rerun_command, "bash scripts/run_ruff.sh lib/lint.py"
        )

    def test_concise_format_is_one_entry_per_line(self) -> None:
        """Every field, and two lines, because one line hides a `break`.

        The mutmut catalog found three survivors here at once: `owner` and
        `log` were unasserted, and a single-line log cannot tell the
        loop's `continue` from a `break`.
        """
        first, second = ruff.parse_failures(
            _log(
                "lib/lint.py:9:2: F821 Undefined name `missing`\n"
                "lib/other.py:3:1: E501 Line too long"
            )
        ).failures

        self.assertEqual(first.identity, "lib/lint.py:9:2")
        self.assertEqual(first.owner, "lib/lint.py")
        self.assertEqual(first.detail, "F821 Undefined name `missing`")
        self.assertEqual(first.log, "phase.log")
        self.assertEqual(
            first.rerun_command, "bash scripts/run_ruff.sh lib/lint.py"
        )
        self.assertEqual(second.identity, "lib/other.py:3:1")
        self.assertEqual(second.owner, "lib/other.py")

    def test_a_header_with_no_location_contributes_nothing(self) -> None:
        """`full` prints help text and source between violations."""
        parsed = ruff.parse_failures(
            _log("F821 Undefined name `missing`\nhelp: define it")
        )

        self.assertEqual(parsed.failures, ())

    def test_a_location_with_no_header_contributes_nothing(self) -> None:
        """Nothing is pending, so there is no code to attach it to.

        The state has to start at `None` for that: the catalog's
        `pending = ""` mutant survived because no log reached a location
        line before a header, and an empty-string sentinel would crash
        unpacking it rather than skip it.
        """
        parsed = ruff.parse_failures(_log(" --> lib/orphan.py:1:1"))

        self.assertEqual(parsed.failures, ())

    def test_the_pending_header_is_consumed_not_reused(self) -> None:
        """One header pairs with one location, never with the next as well."""
        parsed = ruff.parse_failures(
            _log(
                "F821 first\n"
                " --> lib/a.py:1:1\n"
                " --> lib/b.py:2:2\n"
                "E501 second\n"
                " --> lib/c.py:3:3"
            )
        )

        self.assertEqual(
            [(f.identity, f.detail) for f in parsed.failures],
            [("lib/a.py:1:1", "F821 first"), ("lib/c.py:3:3", "E501 second")],
        )

    def test_a_rerun_path_needing_quoting_is_quoted(self) -> None:
        (failure,) = ruff.parse_failures(
            _log("lib/od d.py:1:1: F821 Undefined name `missing`")
        ).failures

        self.assertEqual(
            failure.rerun_command, "bash scripts/run_ruff.sh 'lib/od d.py'"
        )


class TestVultureDialect(unittest.TestCase):
    """`find_dead_code.sh` — its own findings plus the freshness diff."""

    def test_a_finding_becomes_one_entry_at_its_line(self) -> None:
        (failure,) = dead_code.parse_failures(
            _log(
                "lib/dead.py:12: unused function 'orphan' (60% confidence)",
                rerun="bash scripts/find_dead_code.sh",
            )
        ).failures

        self.assertEqual(failure.identity, "lib/dead.py:12")
        self.assertEqual(failure.owner, "lib/dead.py")
        self.assertEqual(
            failure.detail, "unused function 'orphan' (60% confidence)"
        )
        self.assertEqual(
            failure.rerun_command, "bash scripts/find_dead_code.sh"
        )

    def test_a_freshness_diff_line_names_the_identifier_that_moved(
        self,
    ) -> None:
        (failure,) = dead_code.parse_failures(
            _log("+CODEC_TO_EXT  # unused variable (lib/quality/filetypes.py:187)")
        ).failures

        self.assertEqual(failure.identity, "lib/quality/filetypes.py:187")
        self.assertEqual(failure.owner, "lib/quality/filetypes.py")
        self.assertEqual(failure.detail, "CODEC_TO_EXT: unused variable")

    def test_a_removed_whitelist_line_is_not_a_finding(self) -> None:
        """Only additions to the candidate baseline are indexed."""
        parsed = dead_code.parse_failures(
            _log("-GONE  # unused variable (lib/old.py:3)")
        )

        self.assertEqual(parsed.failures, ())

    def test_both_reports_can_appear_in_one_log(self) -> None:
        parsed = dead_code.parse_failures(
            _log(
                "lib/dead.py:12: unused function 'orphan' (60% confidence)\n"
                "+NAME  # unused variable (lib/other.py:4)"
            )
        )

        self.assertEqual(
            [failure.identity for failure in parsed.failures],
            ["lib/dead.py:12", "lib/other.py:4"],
        )


class TestPythonSchedulerDialect(unittest.TestCase):
    """`run_python_tests.py` — typed markers it writes and this decodes."""

    def _marker(self, prefix: str, payload: msgspec.Struct) -> str:
        return prefix + msgspec.json.encode(payload).decode()

    def test_a_failure_marker_reruns_the_exact_failing_test_ids(self) -> None:
        (failure,) = python_tests.parse_failures(
            _log(
                self._marker(
                    python_tests.FAILURE_MARKER_PREFIX,
                    python_tests.CheckFailureMarker(
                        identity="tests.test_alpha.TestAlpha.test_bad",
                        owner="tests/test_alpha.py",
                        detail="assertion failed",
                        test_ids=(
                            "tests.test_alpha.TestAlpha.test_bad",
                            "tests.test_alpha.TestAlpha.test_worse",
                        ),
                    ),
                )
            )
        ).failures

        self.assertEqual(
            failure.rerun_command,
            "python3 -m unittest tests.test_alpha.TestAlpha.test_bad "
            "tests.test_alpha.TestAlpha.test_worse",
        )
        self.assertEqual(len(failure.test_ids), 2)

    def test_a_marker_without_test_ids_falls_back_to_the_phase_command(
        self,
    ) -> None:
        (failure,) = python_tests.parse_failures(
            _log(
                self._marker(
                    python_tests.FAILURE_MARKER_PREFIX,
                    python_tests.CheckFailureMarker(
                        identity="phase", owner="", detail="exploded"
                    ),
                ),
                rerun="python3 scripts/run_python_tests.py",
            )
        ).failures

        self.assertEqual(
            failure.rerun_command, "python3 scripts/run_python_tests.py"
        )
        self.assertEqual(failure.test_ids, ())

    def test_a_metrics_marker_reaches_the_phase_counts(self) -> None:
        parsed = python_tests.parse_failures(
            _log(
                self._marker(
                    python_tests.METRICS_MARKER_PREFIX,
                    python_tests.CheckMetricsMarker(
                        tests_run=12034, targets_run=214, scheduled_targets=220
                    ),
                )
            )
        )

        self.assertEqual(parsed.tests_run, 12034)
        self.assertEqual(parsed.targets_run, 214)
        self.assertEqual(parsed.scheduled_targets, 220)
        self.assertEqual(parsed.failures, ())

    def test_a_later_metrics_marker_replaces_an_earlier_one(self) -> None:
        parsed = python_tests.parse_failures(
            _log(
                self._marker(
                    python_tests.METRICS_MARKER_PREFIX,
                    python_tests.CheckMetricsMarker(tests_run=1),
                )
                + "\n"
                + self._marker(
                    python_tests.METRICS_MARKER_PREFIX,
                    python_tests.CheckMetricsMarker(tests_run=2),
                )
            )
        )

        self.assertEqual(parsed.tests_run, 2)

    def test_a_marker_of_the_wrong_shape_raises_at_the_boundary(self) -> None:
        """Strict decoding is the point: a drifted marker is not dropped."""
        with self.assertRaises(msgspec.ValidationError):
            python_tests.parse_failures(
                _log(
                    python_tests.FAILURE_MARKER_PREFIX
                    + '{"identity": 7, "owner": "", "detail": ""}'
                )
            )

    def test_a_marker_that_is_not_json_raises_at_the_boundary(self) -> None:
        with self.assertRaises(msgspec.DecodeError):
            python_tests.parse_failures(
                _log(python_tests.FAILURE_MARKER_PREFIX + "not json")
            )


class TestDialectsDoNotOverlap(unittest.TestCase):
    """Each parser reads its own tool's output and nobody else's.

    A phase names exactly one parser, so a dialect that also matched a
    neighbour's output would only ever mislead — and the ruff header
    pattern (`CODE message`) is loose enough that this is worth pinning
    rather than assuming.
    """

    OTHER_TOOLS = (
        "CRATEDIGGER_JS_FAILURE\ttests/test_js_a.mjs::s::one\tboom",
        "lib/typed.py:7:4 - error: Argument is unknown",
        "lib/dead.py:12: unused function 'orphan' (60% confidence)",
        (
            "CRATEDIGGER_CHECK_FAILURE "
            '{"identity": "a", "owner": "b", "detail": "c"}'
        ),
    )

    def test_pyright_reads_nothing_from_another_tools_output(self) -> None:
        for text in self.OTHER_TOOLS:
            with self.subTest(text=text):
                if " - error: " in text:
                    continue
                self.assertEqual(
                    pyright_checks.parse_failures(_log(text)).failures, ()
                )

    def test_vulture_reads_nothing_from_another_tools_output(self) -> None:
        for text in self.OTHER_TOOLS:
            with self.subTest(text=text):
                if "confidence)" in text:
                    continue
                self.assertEqual(
                    dead_code.parse_failures(_log(text)).failures, ()
                )

    def test_the_python_scheduler_reads_nothing_from_another_tool(
        self,
    ) -> None:
        for text in self.OTHER_TOOLS:
            with self.subTest(text=text):
                if text.startswith(python_tests.FAILURE_MARKER_PREFIX):
                    continue
                self.assertEqual(
                    python_tests.parse_failures(_log(text)).failures, ()
                )


if __name__ == "__main__":
    unittest.main()
