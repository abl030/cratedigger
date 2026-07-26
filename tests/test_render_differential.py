"""Deterministic pins for the live-corpus render differential harness."""

from __future__ import annotations

import contextlib
import datetime
import io
import json
import os
import sys
import unittest
from collections.abc import Iterator
from pathlib import Path
from tempfile import TemporaryDirectory

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import msgspec  # noqa: E402
import msgspec.inspect  # noqa: E402

from lib.pipeline_db.download_log import _DownloadLogMixin  # noqa: E402
from scripts.render_differential import (  # noqa: E402
    DEFAULT_TARGET_SPEC,
    DiffReport,
    FieldChange,
    RenderDifferentialError,
    RenderedRow,
    classify_render_target,
    format_report,
    load_render_target,
    main,
    read_rendered,
    rendered_fields,
    summarize_render_diff,
    text_bearing_field_names,
)
from web.classify import ClassifiedEntry, LogEntry, classify_log_entry  # noqa: E402


def _row(row_id: int, **fields: str | list[str] | None) -> RenderedRow:
    return RenderedRow(id=row_id, fields=dict(fields))


@contextlib.contextmanager
def _quiet() -> Iterator[io.StringIO]:
    """Run the CLI without spraying its report across the suite output."""
    out = io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(out):
        yield out


def upper_verdict_target(row: dict[str, object]) -> RenderedRow:
    """A second render target, resolved through ``--target`` in tests."""
    rendered = classify_render_target(row)
    upper = dict(rendered.fields)
    verdict = upper["verdict"]
    upper["verdict"] = verdict.upper() if isinstance(verdict, str) else verdict
    return RenderedRow(id=rendered.id, fields=upper)


def not_a_render_target(row: dict[str, object]) -> str:
    """A ``--target`` that returns the wrong type, on purpose."""
    return str(row.get("id"))


class TestTextBearingFieldDerivation(unittest.TestCase):
    """The watched field set comes from the output type, never a list."""

    def setUp(self) -> None:
        self.names = text_bearing_field_names(ClassifiedEntry)
        info = msgspec.inspect.type_info(ClassifiedEntry)
        assert isinstance(info, msgspec.inspect.StructType)
        self.declared = tuple(field.encode_name for field in info.fields)

    def test_every_watched_field_is_declared_on_the_struct(self) -> None:
        self.assertTrue(set(self.names) <= set(self.declared))

    def test_declaration_order_is_preserved(self) -> None:
        self.assertEqual(
            list(self.names),
            [name for name in self.declared if name in set(self.names)],
        )

    def test_the_885_evidence_fields_are_all_watched(self) -> None:
        # The four fields #885 proved byte-identical are exactly as
        # load-bearing as the two that changed.
        for name in ("badge", "badge_class", "border_color",
                     "downloaded_label", "verdict", "summary"):
            with self.subTest(field=name):
                self.assertIn(name, self.names)

    def test_list_of_str_fields_are_watched(self) -> None:
        self.assertIn("bad_extensions", self.names)
        self.assertIn("wrong_match_triage_stage_chain", self.names)

    def test_non_text_fields_are_not_watched(self) -> None:
        for name in ("actual_min_bitrate", "spectral_attempted",
                     "spectral_bitrate", "legacy_projection_version",
                     "comparison_basis"):
            with self.subTest(field=name):
                self.assertNotIn(name, self.names)

    def test_every_watched_field_renders_text_on_a_real_entry(self) -> None:
        # Independent oracle: whatever the declared types say, the values
        # the real classifier produces for the watched fields must be text.
        payload: dict[str, object] = msgspec.to_builtins(classify_log_entry(
            LogEntry(id=1, outcome="rejected", beets_scenario="extra_tracks")))
        for name in self.names:
            with self.subTest(field=name):
                value = payload[name]
                self.assertTrue(
                    value is None or isinstance(value, str)
                    or (isinstance(value, list)
                        and all(isinstance(item, str) for item in value)),
                    f"{name} rendered {value!r}")

    def test_non_struct_output_type_fails_closed(self) -> None:
        with self.assertRaises(RenderDifferentialError):
            text_bearing_field_names(LogEntry)  # pyright: ignore[reportArgumentType]

    def test_rendered_fields_rejects_a_non_text_value(self) -> None:
        with self.assertRaises(RenderDifferentialError):
            rendered_fields(
                ClassifiedEntry(
                    badge="b", badge_class="c", border_color="#fff",
                    verdict="v", summary="s", actual_min_bitrate=320,
                ),
                ("actual_min_bitrate",),
            )


class TestSummarizeRenderDiff(unittest.TestCase):
    """The summarizer is a pure function over two rendered corpora."""

    def test_identical_corpora_report_no_change(self) -> None:
        rows = [_row(1, verdict="a", summary="b"),
                _row(2, verdict="c", summary="d")]
        report = summarize_render_diff(rows, list(rows))
        self.assertEqual(report.total_rows, 2)
        self.assertEqual(report.changed_rows, 0)
        self.assertEqual(report.changed_by_field, {"summary": 0, "verdict": 0})
        self.assertEqual(report.samples, [])

    def test_every_field_is_reported_including_the_unchanged_ones(self) -> None:
        # #885's whole point: proving four fields byte-identical is evidence.
        base = [_row(1, badge="Rejected", verdict="old", summary="s")]
        current = [_row(1, badge="Rejected", verdict="new", summary="s")]
        report = summarize_render_diff(base, current)
        self.assertEqual(
            report.changed_by_field, {"badge": 0, "summary": 0, "verdict": 1})

    def test_one_row_changing_two_fields_counts_one_changed_row(self) -> None:
        base = [_row(1, verdict="old", summary="old")]
        current = [_row(1, verdict="new", summary="new")]
        report = summarize_render_diff(base, current)
        self.assertEqual(report.changed_rows, 1)
        self.assertEqual(report.changed_by_field, {"summary": 1, "verdict": 1})

    def test_two_rows_changing_one_field_counts_two_changed_rows(self) -> None:
        base = [_row(1, verdict="old"), _row(2, verdict="old")]
        current = [_row(1, verdict="new"), _row(2, verdict="new")]
        report = summarize_render_diff(base, current)
        self.assertEqual(report.changed_rows, 2)
        self.assertEqual(report.changed_by_field, {"verdict": 2})

    def test_rows_are_matched_by_id_not_by_position(self) -> None:
        base = [_row(1, verdict="a"), _row(2, verdict="b")]
        current = [_row(2, verdict="b"), _row(1, verdict="a")]
        self.assertEqual(summarize_render_diff(base, current).changed_rows, 0)

    def test_list_and_null_valued_changes_are_counted(self) -> None:
        base = [_row(1, bad_extensions=["a.cue"], transfer_message=None)]
        current = [_row(1, bad_extensions=[], transfer_message="Peer said no")]
        report = summarize_render_diff(base, current)
        self.assertEqual(
            report.changed_by_field, {"bad_extensions": 1, "transfer_message": 1})

    def test_samples_are_bounded_per_field_and_carry_both_sides(self) -> None:
        base = [_row(i, verdict="old") for i in range(1, 6)]
        current = [_row(i, verdict=f"new-{i}") for i in range(1, 6)]
        report = summarize_render_diff(base, current, samples_per_field=2)
        self.assertEqual(report.changed_by_field, {"verdict": 5})
        self.assertEqual(len(report.samples), 2)
        self.assertEqual(
            [(s.id, s.base, s.current) for s in report.samples],
            [(1, "old", "new-1"), (2, "old", "new-2")],
        )

    def test_samples_are_ordered_by_field_then_id(self) -> None:
        base = [_row(2, alpha="a", zeta="z"), _row(1, alpha="a", zeta="z")]
        current = [_row(2, alpha="A", zeta="Z"), _row(1, alpha="A", zeta="Z")]
        report = summarize_render_diff(base, current)
        self.assertEqual(
            [(s.field, s.id) for s in report.samples],
            [("alpha", 1), ("alpha", 2), ("zeta", 1), ("zeta", 2)],
        )

    def test_empty_corpora_report_nothing(self) -> None:
        report = summarize_render_diff([], [])
        self.assertEqual(
            (report.total_rows, report.changed_rows, report.changed_by_field),
            (0, 0, {}),
        )


class TestSummarizeFailsClosed(unittest.TestCase):
    """Every shape that could hide a changed row raises instead."""

    def test_row_missing_from_current(self) -> None:
        with self.assertRaises(RenderDifferentialError) as caught:
            summarize_render_diff(
                [_row(1, verdict="a"), _row(2, verdict="b")],
                [_row(1, verdict="a")])
        self.assertIn("2", str(caught.exception))

    def test_row_absent_from_base(self) -> None:
        with self.assertRaises(RenderDifferentialError):
            summarize_render_diff(
                [_row(1, verdict="a")],
                [_row(1, verdict="a"), _row(2, verdict="b")])

    def test_duplicate_id_in_base(self) -> None:
        with self.assertRaises(RenderDifferentialError):
            summarize_render_diff(
                [_row(1, verdict="a"), _row(1, verdict="a")],
                [_row(1, verdict="a")])

    def test_duplicate_id_in_current(self) -> None:
        with self.assertRaises(RenderDifferentialError):
            summarize_render_diff(
                [_row(1, verdict="a")],
                [_row(1, verdict="a"), _row(1, verdict="a")])

    def test_field_set_drift_between_sides(self) -> None:
        with self.assertRaises(RenderDifferentialError) as caught:
            summarize_render_diff(
                [_row(1, verdict="a", summary="b")],
                [_row(1, verdict="a")])
        self.assertIn("summary", str(caught.exception))

    def test_field_set_drift_within_one_side(self) -> None:
        with self.assertRaises(RenderDifferentialError):
            summarize_render_diff(
                [_row(1, verdict="a"), _row(2, verdict="a", summary="b")],
                [_row(1, verdict="a"), _row(2, verdict="a", summary="b")])

    def test_negative_sample_budget(self) -> None:
        with self.assertRaises(ValueError):
            summarize_render_diff([], [], samples_per_field=-1)


def _download_log_row(**overrides: object) -> dict[str, object]:
    """A production-shaped ``download_log`` row from the read seam."""
    row: dict[str, object] = {
        "id": 4242,
        "request_id": 77,
        "outcome": "rejected",
        "created_at": datetime.datetime(
            2026, 7, 26, 4, 30, tzinfo=datetime.timezone.utc),
        "beets_scenario": "mbid_not_found",
        "beets_distance": 0.04,
        "soulseek_username": "peer",
        "album_title": "Beelzebub",
        "artist_name": "Kikagaku Moyo",
        "request_status": "wanted",
    }
    row.update(overrides)
    return row


class TestClassifyRenderTarget(unittest.TestCase):
    """The default target drives the real production render path."""

    def _expected(self, row: dict[str, object]) -> dict[str, object]:
        overlaid = _DownloadLogMixin._overlay_evidence_onto_download_log_row(
            dict(row))
        classified = classify_log_entry(LogEntry.from_row(overlaid))
        return dict(rendered_fields(
            classified, text_bearing_field_names(ClassifiedEntry)))

    def test_rendered_fields_match_the_real_classifier(self) -> None:
        row = _download_log_row()
        rendered = classify_render_target(row)
        self.assertEqual(rendered.id, 4242)
        self.assertEqual(dict(rendered.fields), self._expected(row))

    def test_rendering_does_not_mutate_the_corpus_row(self) -> None:
        row = _download_log_row(_evidence_spectral_grade="genuine")
        before = dict(row)
        classify_render_target(row)
        self.assertEqual(row, before)

    def test_the_production_evidence_overlay_runs(self) -> None:
        # Rejected rows carry their measurement on album_quality_evidence,
        # not on the denorm columns. Without the overlay the spectral fields
        # would render None and the differential would under-cover them.
        with_evidence = classify_render_target(_download_log_row(
            outcome="success",
            spectral_grade=None,
            _evidence_spectral_grade="genuine",
            _evidence_spectral_bitrate=880,
        ))
        without_evidence = classify_render_target(
            _download_log_row(outcome="success", spectral_grade=None))
        self.assertEqual(with_evidence.fields["spectral_grade"], "genuine")
        self.assertIsNone(without_evidence.fields["spectral_grade"])

    def test_row_without_an_integer_id_fails_closed(self) -> None:
        with self.assertRaises(RenderDifferentialError):
            classify_render_target(_download_log_row(id="4242"))

    def test_boolean_id_fails_closed(self) -> None:
        with self.assertRaises(RenderDifferentialError):
            classify_render_target(_download_log_row(id=True))


class TestRenderTargetLoading(unittest.TestCase):
    """``--target`` resolves a dotted spec and validates what it returns."""

    def test_default_spec_returns_the_classify_target(self) -> None:
        self.assertIs(load_render_target(None), classify_render_target)
        self.assertIs(
            load_render_target(DEFAULT_TARGET_SPEC), classify_render_target)

    def test_dotted_spec_is_imported(self) -> None:
        target = load_render_target(
            "tests.test_render_differential:upper_verdict_target")
        rendered = target(_download_log_row())
        expected = classify_render_target(_download_log_row())
        verdict = expected.fields["verdict"]
        assert isinstance(verdict, str)
        self.assertEqual(rendered.fields["verdict"], verdict.upper())

    def test_malformed_spec_fails_closed(self) -> None:
        with self.assertRaises(RenderDifferentialError):
            load_render_target("web.classify")

    def test_unimportable_module_fails_closed(self) -> None:
        with self.assertRaises(RenderDifferentialError):
            load_render_target("lib.no_such_module_888:target")

    def test_missing_attribute_fails_closed(self) -> None:
        with self.assertRaises(RenderDifferentialError):
            load_render_target("web.classify:no_such_function")

    def test_target_returning_the_wrong_type_fails_closed(self) -> None:
        target = load_render_target(
            "tests.test_render_differential:not_a_render_target")
        with self.assertRaises(RenderDifferentialError):
            target(_download_log_row())


class TestCommandLine(unittest.TestCase):
    """render → render → diff, end to end, on real classify output."""

    def setUp(self) -> None:
        self._tmp = TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.dir = Path(self._tmp.name)
        self.corpus = self.dir / "corpus.jsonl"
        self.corpus.write_text(
            "\n".join(
                json.dumps(_download_log_row(id=row_id), default=str)
                for row_id in (1, 2, 3)
            ) + "\n",
            encoding="utf-8",
        )

    def _render(self, name: str, target: str | None = None) -> Path:
        out = self.dir / name
        argv = ["render", "--corpus", str(self.corpus), "--out", str(out)]
        if target is not None:
            argv += ["--target", target]
        with _quiet():
            self.assertEqual(main(argv), 0)
        return out

    def _diff(self, *argv: str) -> tuple[int, str]:
        with _quiet() as out:
            code = main(["diff", *argv])
        return code, out.getvalue()

    def test_identical_renders_report_zero_changes(self) -> None:
        base = self._render("base.jsonl")
        current = self._render("current.jsonl")
        report = summarize_render_diff(
            read_rendered(str(base)), read_rendered(str(current)))
        self.assertEqual(report.total_rows, 3)
        self.assertEqual(report.changed_rows, 0)
        self.assertTrue(report.changed_by_field)
        self.assertEqual(set(report.changed_by_field.values()), {0})
        code, printed = self._diff("--base", str(base), "--current", str(current))
        self.assertEqual(code, 0)
        self.assertIn("changed rows: 0", printed)

    def test_a_changed_target_is_reported_by_field(self) -> None:
        base = self._render("base.jsonl")
        current = self._render(
            "current.jsonl",
            target="tests.test_render_differential:upper_verdict_target")
        report = summarize_render_diff(
            read_rendered(str(base)), read_rendered(str(current)))
        self.assertEqual(report.changed_rows, 3)
        self.assertEqual(report.changed_by_field["verdict"], 3)
        self.assertEqual(report.changed_by_field["badge"], 0)
        self.assertEqual(report.changed_by_field["summary"], 0)

    def test_blank_corpus_lines_are_skipped(self) -> None:
        self.corpus.write_text(
            json.dumps(_download_log_row(id=9), default=str) + "\n\n",
            encoding="utf-8")
        self.assertEqual(len(read_rendered(str(self._render("r.jsonl")))), 1)

    def test_corpus_line_that_is_not_an_object_fails_closed(self) -> None:
        self.corpus.write_text("[1, 2]\n", encoding="utf-8")
        with _quiet():
            self.assertEqual(
                main(["render", "--corpus", str(self.corpus),
                      "--out", str(self.dir / "r.jsonl")]), 1)

    def test_rendered_file_that_is_not_a_rendered_row_fails_closed(
        self,
    ) -> None:
        broken = self.dir / "broken.jsonl"
        broken.write_text('{"id": "one", "fields": {}}\n', encoding="utf-8")
        with self.assertRaises(RenderDifferentialError):
            read_rendered(str(broken))

    def test_diff_over_mismatched_corpora_exits_nonzero(self) -> None:
        base = self._render("base.jsonl")
        short = self.dir / "short.jsonl"
        short.write_text(
            msgspec.json.encode(read_rendered(str(base))[0]).decode() + "\n",
            encoding="utf-8")
        code, printed = self._diff("--base", str(base), "--current", str(short))
        self.assertEqual(code, 1)
        self.assertIn("different rows", printed)

    def test_missing_corpus_file_exits_nonzero(self) -> None:
        with _quiet():
            self.assertEqual(
                main(["render", "--corpus", str(self.dir / "absent.jsonl"),
                      "--out", str(self.dir / "r.jsonl")]), 1)

    def test_negative_sample_budget_is_rejected_by_the_parser(self) -> None:
        base = self._render("base.jsonl")
        with _quiet(), self.assertRaises(SystemExit):
            main(["diff", "--base", str(base), "--current", str(base),
                  "--samples", "-1"])

    def test_json_report_round_trips(self) -> None:
        base = self._render("base.jsonl")
        code, printed = self._diff(
            "--base", str(base), "--current", str(base), "--json")
        self.assertEqual(code, 0)
        decoded = msgspec.json.decode(printed, type=DiffReport)
        self.assertEqual((decoded.total_rows, decoded.changed_rows), (3, 0))


class TestFormatReport(unittest.TestCase):
    """The printed report is the operator-facing evidence."""

    def test_zero_change_report_still_names_every_field(self) -> None:
        text = format_report(DiffReport(
            total_rows=36308,
            changed_rows=0,
            changed_by_field={"badge": 0, "verdict": 0},
            samples=[],
        ))
        self.assertIn("rows: 36308", text)
        self.assertIn("changed rows: 0", text)
        self.assertIn("badge", text)
        self.assertIn("verdict", text)

    def test_samples_show_both_sides(self) -> None:
        text = format_report(DiffReport(
            total_rows=1,
            changed_rows=1,
            changed_by_field={"verdict": 1},
            samples=[FieldChange(
                id=7, field="verdict", base="mbid_not_found",
                current="Requested release ID not among the match candidates")],
        ))
        self.assertIn("[verdict] id=7", text)
        self.assertIn("mbid_not_found", text)
        self.assertIn("Requested release ID not among the match candidates",
                      text)


if __name__ == "__main__":
    unittest.main()
