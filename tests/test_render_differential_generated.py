"""Generated contracts for the live-corpus render differential summary.

The invariants the summarizer exists to uphold:

* **Completeness** — every rendered field appears in the report exactly
  once, zeros included. A field that silently vanishes from the report is
  a field nobody proved byte-identical.
* **Changed-row exactness** — ``changed_rows`` is the number of ids whose
  rendered fields differ in at least one field.
* **Per-field exactness** — each per-field count is the number of ids that
  differ in that field, and is therefore bounded by ``changed_rows``,
  which is bounded by ``total_rows``.
* **Sample honesty** — every sampled before/after pair is a real
  difference at that id and field, and no field is sampled beyond budget.
* **Identity** — a corpus differentialled against itself reports nothing.
* **Fail-closed** — a row on one side only, a repeated id, or field-set
  drift raises instead of being skipped.
"""

from __future__ import annotations

import unittest

from hypothesis import example, given, strategies as st

from scripts.render_differential import (
    DiffReport,
    FieldChange,
    RenderDifferentialError,
    RenderedRow,
    summarize_render_diff,
)
import tests._hypothesis_profiles  # noqa: F401

FIELD_ALPHABET = ("badge", "verdict", "summary", "downloaded_label")

Corpora = tuple[list[RenderedRow], list[RenderedRow]]

_VALUES = st.one_of(
    st.none(),
    st.text(alphabet="abc", max_size=4),
    st.lists(st.text(alphabet="xy", max_size=2), max_size=2),
)


@st.composite
def rendered_corpora(draw: st.DrawFn, *, min_rows: int = 0) -> Corpora:
    """A base/current pair over the same ids and the same field set."""
    names = draw(st.lists(
        st.sampled_from(FIELD_ALPHABET), min_size=1, max_size=4, unique=True))
    ids = draw(st.lists(
        st.integers(min_value=1, max_value=30),
        min_size=min_rows, max_size=10, unique=True))
    base: list[RenderedRow] = []
    current: list[RenderedRow] = []
    for row_id in ids:
        base_fields = {name: draw(_VALUES) for name in names}
        current_fields = {
            name: (
                base_fields[name] if draw(st.booleans()) else draw(_VALUES)
            )
            for name in names
        }
        base.append(RenderedRow(id=row_id, fields=base_fields))
        current.append(RenderedRow(id=row_id, fields=current_fields))
    # Row order must not matter: the summarizer keys on id.
    return base, list(draw(st.permutations(current)))


# --- Invariant checkers (module-level so the self-tests can call them) -----

def _fields_by_id(rows: list[RenderedRow]) -> dict[int, dict[str, object]]:
    return {row.id: dict(row.fields) for row in rows}


def check_report_covers_every_field(
    base: list[RenderedRow], report: DiffReport,
) -> None:
    expected = set(base[0].fields) if base else set()
    if set(report.changed_by_field) != expected:
        raise AssertionError(
            "report field coverage drifted: "
            f"missing {sorted(expected - set(report.changed_by_field))}, "
            f"extra {sorted(set(report.changed_by_field) - expected)}")


def check_totals(
    base: list[RenderedRow], current: list[RenderedRow], report: DiffReport,
) -> None:
    if report.total_rows != len(base) or report.total_rows != len(current):
        raise AssertionError(
            f"total_rows {report.total_rows} does not match the corpora "
            f"({len(base)} base, {len(current)} current)")


def check_changed_rows_exact(
    base: list[RenderedRow], current: list[RenderedRow], report: DiffReport,
) -> None:
    current_fields = _fields_by_id(current)
    expected = sum(
        1 for row in base if dict(row.fields) != current_fields[row.id])
    if report.changed_rows != expected:
        raise AssertionError(
            f"changed_rows {report.changed_rows} != {expected} rows that "
            "actually differ")


def check_field_counts_exact(
    base: list[RenderedRow], current: list[RenderedRow], report: DiffReport,
) -> None:
    current_fields = _fields_by_id(current)
    for name, count in report.changed_by_field.items():
        expected = sum(
            1 for row in base
            if row.fields[name] != current_fields[row.id][name])
        if count != expected:
            raise AssertionError(
                f"field {name!r} counted {count} changed rows, {expected} "
                "actually differ")


def check_counts_bounded(report: DiffReport) -> None:
    if not 0 <= report.changed_rows <= report.total_rows:
        raise AssertionError(
            f"changed_rows {report.changed_rows} is outside "
            f"[0, {report.total_rows}]")
    for name, count in report.changed_by_field.items():
        if not 0 <= count <= report.changed_rows:
            raise AssertionError(
                f"field {name!r} count {count} is outside "
                f"[0, {report.changed_rows}]")
    total = sum(report.changed_by_field.values())
    if report.changed_rows and total < report.changed_rows:
        raise AssertionError(
            f"{report.changed_rows} changed rows but only {total} field "
            "changes: some changed row has no changed field")
    if not report.changed_rows and total:
        raise AssertionError(
            f"no changed rows but {total} field changes reported")


def check_samples_are_real(
    base: list[RenderedRow],
    current: list[RenderedRow],
    report: DiffReport,
    budget: int,
) -> None:
    base_fields = _fields_by_id(base)
    current_fields = _fields_by_id(current)
    per_field: dict[str, int] = {}
    for sample in report.samples:
        if sample.id not in base_fields:
            raise AssertionError(f"sample names unknown row {sample.id}")
        if sample.field not in base_fields[sample.id]:
            raise AssertionError(f"sample names unknown field {sample.field!r}")
        if sample.base != base_fields[sample.id][sample.field]:
            raise AssertionError(
                f"sample base value for row {sample.id} field "
                f"{sample.field!r} is not what base rendered")
        if sample.current != current_fields[sample.id][sample.field]:
            raise AssertionError(
                f"sample current value for row {sample.id} field "
                f"{sample.field!r} is not what current rendered")
        if sample.base == sample.current:
            raise AssertionError(
                f"sample for row {sample.id} field {sample.field!r} is not a "
                "change")
        per_field[sample.field] = per_field.get(sample.field, 0) + 1
    for name, sampled in per_field.items():
        if sampled > budget:
            raise AssertionError(
                f"field {name!r} sampled {sampled} times, budget {budget}")
        if sampled > report.changed_by_field[name]:
            raise AssertionError(
                f"field {name!r} sampled {sampled} times but only "
                f"{report.changed_by_field[name]} rows changed")


_ONE_CHANGED_FIELD: Corpora = (
    [RenderedRow(id=1, fields={"verdict": "mbid_not_found"})],
    [RenderedRow(id=1, fields={"verdict": "Requested release ID not among "
                                          "the match candidates"})],
)
_TWO_ROWS_ONE_CHANGED: Corpora = (
    [RenderedRow(id=1, fields={"badge": "Rejected", "verdict": "a"}),
     RenderedRow(id=2, fields={"badge": "Rejected", "verdict": "b"})],
    [RenderedRow(id=1, fields={"badge": "Rejected", "verdict": "a"}),
     RenderedRow(id=2, fields={"badge": "Rejected", "verdict": "B"})],
)


class TestRenderDiffProperties(unittest.TestCase):
    """The summarizer's report is a faithful census of the two corpora."""

    @given(corpora=rendered_corpora(), budget=st.integers(0, 3))
    @example(corpora=_ONE_CHANGED_FIELD, budget=3)
    @example(corpora=_TWO_ROWS_ONE_CHANGED, budget=3)
    def test_report_is_an_exact_census(
        self, corpora: Corpora, budget: int,
    ) -> None:
        base, current = corpora
        report = summarize_render_diff(
            base, current, samples_per_field=budget)
        check_totals(base, current, report)
        check_report_covers_every_field(base, report)
        check_changed_rows_exact(base, current, report)
        check_field_counts_exact(base, current, report)
        check_counts_bounded(report)
        check_samples_are_real(base, current, report, budget)

    @given(corpora=rendered_corpora())
    def test_a_corpus_against_itself_reports_nothing(
        self, corpora: Corpora,
    ) -> None:
        base, _ = corpora
        report = summarize_render_diff(base, list(base))
        self.assertEqual(report.changed_rows, 0)
        self.assertEqual(report.samples, [])
        self.assertEqual(set(report.changed_by_field.values()) - {0}, set())
        check_report_covers_every_field(base, report)

    @given(corpora=rendered_corpora())
    def test_summary_is_deterministic(self, corpora: Corpora) -> None:
        base, current = corpora
        self.assertEqual(
            summarize_render_diff(base, current),
            summarize_render_diff(base, current))


class TestRenderDiffFailsClosed(unittest.TestCase):
    """A corpus that could hide a changed row is rejected, never skipped."""

    @given(corpora=rendered_corpora(min_rows=1))
    def test_row_missing_from_current(self, corpora: Corpora) -> None:
        base, current = corpora
        with self.assertRaises(RenderDifferentialError):
            summarize_render_diff(base, current[1:])

    @given(corpora=rendered_corpora(min_rows=1))
    def test_row_absent_from_base(self, corpora: Corpora) -> None:
        base, current = corpora
        extra = RenderedRow(id=999, fields=dict(current[0].fields))
        with self.assertRaises(RenderDifferentialError):
            summarize_render_diff(base, [*current, extra])

    @given(corpora=rendered_corpora(min_rows=1))
    def test_repeated_id(self, corpora: Corpora) -> None:
        base, current = corpora
        with self.assertRaises(RenderDifferentialError):
            summarize_render_diff(base, [*current, current[0]])

    @given(corpora=rendered_corpora(min_rows=1))
    def test_field_dropped_from_current(self, corpora: Corpora) -> None:
        base, current = corpora
        dropped = sorted(base[0].fields)[0]
        thinned = [
            RenderedRow(
                id=row.id,
                fields={k: v for k, v in row.fields.items() if k != dropped},
            )
            for row in current
        ]
        with self.assertRaises(RenderDifferentialError):
            summarize_render_diff(base, thinned)

    @given(corpora=rendered_corpora(min_rows=2))
    def test_field_set_drift_inside_one_side(self, corpora: Corpora) -> None:
        base, current = corpora
        widened = [*base[:-1], RenderedRow(
            id=base[-1].id, fields={**base[-1].fields, "extra": "x"})]
        with self.assertRaises(RenderDifferentialError):
            summarize_render_diff(widened, current)


def _dropping_summarizer(
    base: list[RenderedRow], current: list[RenderedRow], budget: int,
) -> DiffReport:
    """A summarizer that silently drops the field that changed the most."""
    honest = summarize_render_diff(base, current, samples_per_field=budget)
    if not honest.changed_by_field:
        return honest
    worst = max(honest.changed_by_field, key=lambda k: honest.changed_by_field[k])
    return DiffReport(
        total_rows=honest.total_rows,
        changed_rows=honest.changed_rows,
        changed_by_field={
            k: v for k, v in honest.changed_by_field.items() if k != worst},
        samples=[s for s in honest.samples if s.field != worst],
    )


class TestInvariantCheckersTripOnViolations(unittest.TestCase):
    """Planted violations prove each checker constrains the summary."""

    def setUp(self) -> None:
        self.base, self.current = _TWO_ROWS_ONE_CHANGED
        self.report = summarize_render_diff(self.base, self.current)

    def test_a_summarizer_that_drops_a_changed_field_is_caught(self) -> None:
        dropped = _dropping_summarizer(self.base, self.current, 3)
        with self.assertRaises(AssertionError):
            check_report_covers_every_field(self.base, dropped)
        # The dropped field is also missing from the per-field census, so
        # the count checker can no longer see the rows it hid.
        self.assertNotIn("verdict", dropped.changed_by_field)

    def test_a_dropped_unchanged_field_is_caught_too(self) -> None:
        # The zeros carry the "byte-identical" evidence; losing one is the
        # same defect as losing a changed field.
        thinned = DiffReport(
            total_rows=self.report.total_rows,
            changed_rows=self.report.changed_rows,
            changed_by_field={
                k: v for k, v in self.report.changed_by_field.items()
                if k != "badge"},
            samples=list(self.report.samples),
        )
        with self.assertRaises(AssertionError):
            check_report_covers_every_field(self.base, thinned)

    def test_an_undercounted_field_is_caught(self) -> None:
        undercounted = DiffReport(
            total_rows=self.report.total_rows,
            changed_rows=self.report.changed_rows,
            changed_by_field={**self.report.changed_by_field, "verdict": 0},
            samples=list(self.report.samples),
        )
        with self.assertRaises(AssertionError):
            check_field_counts_exact(self.base, self.current, undercounted)

    def test_an_understated_changed_row_count_is_caught(self) -> None:
        understated = DiffReport(
            total_rows=self.report.total_rows,
            changed_rows=0,
            changed_by_field=dict(self.report.changed_by_field),
            samples=list(self.report.samples),
        )
        with self.assertRaises(AssertionError):
            check_changed_rows_exact(self.base, self.current, understated)

    def test_a_wrong_total_is_caught(self) -> None:
        wrong = DiffReport(
            total_rows=self.report.total_rows + 1,
            changed_rows=self.report.changed_rows,
            changed_by_field=dict(self.report.changed_by_field),
            samples=list(self.report.samples),
        )
        with self.assertRaises(AssertionError):
            check_totals(self.base, self.current, wrong)

    def test_a_count_above_the_changed_row_total_is_caught(self) -> None:
        inflated = DiffReport(
            total_rows=self.report.total_rows,
            changed_rows=self.report.changed_rows,
            changed_by_field={
                **self.report.changed_by_field,
                "badge": self.report.changed_rows + 1},
            samples=list(self.report.samples),
        )
        with self.assertRaises(AssertionError):
            check_counts_bounded(inflated)

    def test_changes_reported_without_a_changed_row_are_caught(self) -> None:
        # Must-still-work: a genuinely unchanged corpus stays consistent.
        check_counts_bounded(DiffReport(
            total_rows=2, changed_rows=0,
            changed_by_field={"verdict": 0, "badge": 0}, samples=[]))
        with self.assertRaises(AssertionError):
            check_counts_bounded(DiffReport(
                total_rows=2, changed_rows=0,
                changed_by_field={"verdict": 1, "badge": 0}, samples=[]))

    def test_a_changed_row_with_no_changed_field_is_caught(self) -> None:
        with self.assertRaises(AssertionError):
            check_counts_bounded(DiffReport(
                total_rows=2, changed_rows=1,
                changed_by_field={"verdict": 0, "badge": 0}, samples=[]))

    def test_a_fabricated_sample_is_caught(self) -> None:
        fabricated = DiffReport(
            total_rows=self.report.total_rows,
            changed_rows=self.report.changed_rows,
            changed_by_field=dict(self.report.changed_by_field),
            samples=[FieldChange(
                id=2, field="verdict", base="never rendered",
                current="also never rendered")],
        )
        with self.assertRaises(AssertionError):
            check_samples_are_real(self.base, self.current, fabricated, 3)

    def test_an_oversampled_field_is_caught(self) -> None:
        oversampled = DiffReport(
            total_rows=self.report.total_rows,
            changed_rows=self.report.changed_rows,
            changed_by_field=dict(self.report.changed_by_field),
            samples=[
                FieldChange(id=2, field="verdict", base="b", current="B"),
                FieldChange(id=2, field="verdict", base="b", current="B"),
            ],
        )
        with self.assertRaises(AssertionError):
            check_samples_are_real(self.base, self.current, oversampled, 1)


if __name__ == "__main__":
    unittest.main()
