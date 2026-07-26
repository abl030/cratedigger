"""Generated contracts for the live-corpus render differential.

Two halves, and the second one is the half that was missing when a
fail-open derivation let ``comparison_basis`` go unwatched.

**The summary** — ``summarize_render_diff`` is an exact census of two
rendered corpora:

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
* **Fail-closed** — a row on one side only, a repeated id, or unsanctioned
  field-set drift raises instead of being skipped.

**The watched-field derivation** — what the differential looks at:

* **Fail-closed derivation** — a field is unwatched only when its declared
  type is provably built from numbers, booleans and null. Strings, nested
  Structs, ``object``, and string-keyed mappings are all watched.
* **Converse** — driving the REAL Recents render path over generated rows,
  no unwatched field ever holds text at runtime.
"""

from __future__ import annotations

import unittest
from typing import NamedTuple

from hypothesis import example, given, strategies as st

import msgspec

from scripts.render_differential import (
    ClassifyRenderTarget,
    DiffReport,
    FieldChange,
    RenderDifferentialError,
    RenderedRow,
    contains_text,
    summarize_render_diff,
    unwatched_field_names,
    watched_field_names,
)
import tests._hypothesis_profiles  # noqa: F401
from web.classify import ClassifiedEntry
from web.routes.pipeline import _classify_pipeline_log_item

FIELD_ALPHABET = ("badge", "verdict", "summary", "comparison_basis")

Corpora = tuple[list[RenderedRow], list[RenderedRow]]

_VALUES = st.one_of(
    st.none(),
    st.text(alphabet="abc", max_size=4),
    st.lists(st.text(alphabet="xy", max_size=2), max_size=2),
    st.dictionaries(
        st.sampled_from(["verdict", "new_rank"]),
        st.one_of(st.text(alphabet="ab", max_size=2), st.integers(0, 3)),
        max_size=2),
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
        base_fields: dict[str, object] = {
            name: draw(_VALUES) for name in names}
        current_fields: dict[str, object] = {
            name: (
                base_fields[name] if draw(st.booleans()) else draw(_VALUES)
            )
            for name in names
        }
        base.append(RenderedRow(id=row_id, fields=base_fields))
        current.append(RenderedRow(id=row_id, fields=current_fields))
    # Row order must not matter: the summarizer keys on id.
    return base, list(draw(st.permutations(current)))


# --- Summary invariant checkers (module-level for the self-tests) ---------

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
_NESTED_BASIS_NULLED: Corpora = (
    [RenderedRow(id=1, fields={"comparison_basis": {"verdict": "worse"}})],
    [RenderedRow(id=1, fields={"comparison_basis": None})],
)


class TestRenderDiffProperties(unittest.TestCase):
    """The summarizer's report is a faithful census of the two corpora."""

    @given(corpora=rendered_corpora(), budget=st.integers(0, 3))
    @example(corpora=_ONE_CHANGED_FIELD, budget=3)
    @example(corpora=_TWO_ROWS_ONE_CHANGED, budget=3)
    @example(corpora=_NESTED_BASIS_NULLED, budget=3)
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

    @given(corpora=rendered_corpora(min_rows=1))
    def test_dropped_field_is_named_when_drift_is_allowed(
        self, corpora: Corpora,
    ) -> None:
        base, current = corpora
        dropped = sorted(base[0].fields)[0]
        thinned = [
            RenderedRow(
                id=row.id,
                fields={k: v for k, v in row.fields.items() if k != dropped},
            )
            for row in current
        ]
        report = summarize_render_diff(base, thinned, allow_field_drift=True)
        self.assertEqual(report.base_only_fields, [dropped])
        self.assertNotIn(dropped, report.changed_by_field)

    @given(corpora=rendered_corpora(min_rows=2))
    def test_field_set_drift_inside_one_side(self, corpora: Corpora) -> None:
        base, current = corpora
        widened = [*base[:-1], RenderedRow(
            id=base[-1].id, fields={**base[-1].fields, "extra": "x"})]
        with self.assertRaises(RenderDifferentialError):
            summarize_render_diff(widened, current)


# --- Watched-field derivation ---------------------------------------------

class _NestedText(msgspec.Struct):
    """A nested Struct carrying text a fail-open derivation would miss."""

    note: str


class _NestedNumbers(msgspec.Struct):
    """A nested Struct with no text — still watched, because fail-closed."""

    count: int


class Shape(NamedTuple):
    """One generated field type plus the truth about it.

    ``annotation`` is an arbitrary type expression (``int``, ``str | None``,
    ``dict[str, object]``, a Struct class), which has no single static type
    — it is handed straight to ``msgspec.defstruct``.
    """

    annotation: object
    provably_non_text: bool


@st.composite
def shapes(draw: st.DrawFn, depth: int = 2) -> Shape:
    """A declared field type, carrying its own provably-non-text oracle."""
    leaves = ["int", "float", "bool", "none", "str", "object",
              "nested_text", "nested_numbers"]
    containers = ["optional", "list", "tuple", "dict_str", "dict_int"]
    kind = draw(st.sampled_from(leaves + (containers if depth else [])))
    if kind == "int":
        return Shape(int, True)
    if kind == "float":
        return Shape(float, True)
    if kind == "bool":
        return Shape(bool, True)
    if kind == "none":
        return Shape(type(None), True)
    if kind == "str":
        return Shape(str, False)
    if kind == "object":
        return Shape(object, False)
    if kind == "nested_text":
        return Shape(_NestedText, False)
    if kind == "nested_numbers":
        # No text anywhere inside, but a Struct is never PROVABLY text-free
        # to the derivation, so fail-closed keeps it watched.
        return Shape(_NestedNumbers, False)
    inner = draw(shapes(depth - 1))
    if kind == "optional":
        return Shape(
            inner.annotation | None,  # pyright: ignore[reportOperatorIssue]
            inner.provably_non_text)
    if kind == "list":
        return Shape(list[inner.annotation], inner.provably_non_text)
    if kind == "tuple":
        return Shape(tuple[inner.annotation, int], inner.provably_non_text)
    if kind == "dict_str":
        # String keys are operator-visible text on their own.
        return Shape(dict[str, inner.annotation], False)
    return Shape(dict[int, inner.annotation], inner.provably_non_text)


def check_derivation_matches_the_shapes(
    shape_by_name: dict[str, Shape],
    watched: tuple[str, ...],
    unwatched: tuple[str, ...],
) -> None:
    """Watched/unwatched must partition the Struct and match each oracle."""
    if set(watched) | set(unwatched) != set(shape_by_name):
        raise AssertionError(
            "derivation lost fields: "
            f"{sorted(set(shape_by_name) - set(watched) - set(unwatched))}")
    if set(watched) & set(unwatched):
        raise AssertionError(
            f"field in both sets: {sorted(set(watched) & set(unwatched))}")
    for name, shape in shape_by_name.items():
        if shape.provably_non_text and name not in unwatched:
            raise AssertionError(
                f"{name} ({shape.annotation}) is numeric but was watched")
        if not shape.provably_non_text and name not in watched:
            raise AssertionError(
                f"{name} ({shape.annotation}) may carry text but was NOT "
                "watched — the differential would under-report it")


class TestWatchedFieldDerivationProperties(unittest.TestCase):
    """The derivation fails closed on every declared shape."""

    @given(shape_list=st.lists(shapes(), min_size=1, max_size=6))
    @example(shape_list=[Shape(dict[str, object], False)])
    @example(shape_list=[Shape(_NestedText | None, False)])
    @example(shape_list=[Shape(object, False)])
    def test_derivation_matches_the_declared_shapes(
        self, shape_list: list[Shape],
    ) -> None:
        shape_by_name = {
            f"f{index}": shape for index, shape in enumerate(shape_list)}
        struct = msgspec.defstruct(
            "GeneratedOutput",
            [  # pyright: ignore[reportArgumentType]
                (name, shape.annotation)
                for name, shape in shape_by_name.items()
            ],
        )
        check_derivation_matches_the_shapes(
            shape_by_name,
            watched_field_names(struct),
            unwatched_field_names(struct),
        )


# --- Converse oracle over the real Recents render path --------------------

_OUTCOMES = (
    "success", "rejected", "failed", "timeout", "measurement_failed",
    "force_import", "curator_ban", "user_offline", "have_analysis_error",
    "manual_import", "found",
)
_SCENARIOS = (
    None, "mbid_not_found", "extra_tracks", "strong_match", "import_failed",
    "untracked_audio", "transcode_upgrade", "transcode_first",
    "audio_corrupt", "downgrade",
)


@st.composite
def download_log_rows(draw: st.DrawFn) -> dict[str, object]:
    """A generated ``download_log`` row in the production read-seam shape."""
    row: dict[str, object] = {
        "id": draw(st.integers(1, 5000)),
        "request_id": draw(st.integers(1, 500)),
        "outcome": draw(st.sampled_from(_OUTCOMES)),
        "created_at": "2026-07-26T04:30:00+00:00",
        "beets_scenario": draw(st.sampled_from(_SCENARIOS)),
        "beets_distance": draw(st.one_of(st.none(), st.floats(0, 1))),
        "soulseek_username": draw(
            st.one_of(st.none(), st.text(alphabet="peru", max_size=4))),
        "error_message": draw(
            st.one_of(st.none(), st.text(alphabet="err ", max_size=8))),
        "beets_detail": draw(
            st.one_of(st.none(), st.text(alphabet="dt ", max_size=8))),
        "album_title": draw(st.text(alphabet="ab ", max_size=6)),
        "artist_name": draw(st.text(alphabet="cd ", max_size=6)),
        "request_status": draw(
            st.sampled_from(["wanted", "imported", "unsearchable", "replaced"])),
        "was_converted": draw(st.booleans()),
        "original_filetype": draw(st.sampled_from([None, "flac", "mp3"])),
        "actual_filetype": draw(st.sampled_from([None, "mp3", "flac"])),
        "actual_min_bitrate": draw(st.one_of(st.none(), st.integers(0, 1200))),
        "existing_min_bitrate": draw(st.one_of(st.none(), st.integers(0, 1200))),
        "spectral_grade": draw(
            st.sampled_from([None, "genuine", "transparent", "suspect"])),
        "spectral_bitrate": draw(st.one_of(st.none(), st.integers(0, 1200))),
        "search_filetype_override": draw(st.sampled_from([None, "flac"])),
    }
    if draw(st.booleans()):
        row["_evidence_spectral_grade"] = draw(
            st.sampled_from(["genuine", "transparent"]))
        row["_evidence_lineage_version"] = draw(st.sampled_from([1, 3, 4]))
        row["_evidence_source_format"] = draw(st.sampled_from(["MP3", "FLAC"]))
        row["_evidence_source_min_bitrate"] = draw(st.integers(64, 1200))
    if draw(st.booleans()):
        row["_current_evidence_id"] = draw(st.integers(1, 99))
        row["_current_evidence_is_pre_attempt"] = draw(st.booleans())
        row["_current_evidence_format"] = draw(
            st.sampled_from([None, "MP3", "FLAC"]))
        row["_current_evidence_min_bitrate"] = draw(
            st.one_of(st.none(), st.integers(64, 1200)))
        row["_current_evidence_avg_bitrate"] = draw(
            st.one_of(st.none(), st.integers(64, 1200)))
        row["_current_evidence_spectral_grade"] = draw(
            st.sampled_from([None, "genuine", "transparent"]))
        row["_current_evidence_v0_probe_kind"] = draw(
            st.sampled_from([None, "lossless_source_v0"]))
    if draw(st.booleans()):
        row["import_result"] = {
            "decision": draw(st.sampled_from(
                ["import", "downgrade", "audio_corrupt", "import_no_exist"])),
            "comparison_basis": {
                "verdict": draw(st.sampled_from(["better", "worse", "equivalent"])),
                "branch": draw(st.sampled_from(["rank", "metric_tiebreak"])),
                "new_rank": "good",
                "existing_rank": "acceptable",
                "new_format": "MP3",
                "existing_format": "MP3",
            },
        }
    return row


def check_no_unwatched_text(
    item: dict[str, object],
    unwatched: tuple[str, ...],
) -> None:
    """No field the derivation ignored may hold text at runtime."""
    for name in unwatched:
        if name in item and contains_text(item[name]):
            raise AssertionError(
                f"unwatched field {name!r} holds text {item[name]!r}")


def check_every_watched_field_rendered(
    rendered: RenderedRow, watched: tuple[str, ...],
) -> None:
    if set(rendered.fields) != set(watched):
        raise AssertionError(
            "rendered field set does not match the watched set: "
            f"missing {sorted(set(watched) - set(rendered.fields))}, "
            f"extra {sorted(set(rendered.fields) - set(watched))}")


class TestRealRenderPathConverse(unittest.TestCase):
    """Driving the real Recents render path, text stays inside the set."""

    WATCHED = watched_field_names(ClassifiedEntry)
    UNWATCHED = unwatched_field_names(ClassifiedEntry)

    @given(row=download_log_rows())
    def test_no_unwatched_field_holds_text(
        self, row: dict[str, object],
    ) -> None:
        target = ClassifyRenderTarget()
        target.prepare([row])
        rendered = target.render(row)
        check_every_watched_field_rendered(rendered, self.WATCHED)
        # The projection enforces this too; recompute it independently over
        # the full production item so the property is not merely restating
        # the code under test.
        check_no_unwatched_text(
            _classify_pipeline_log_item(row), self.UNWATCHED)

    @given(origin=download_log_rows(), successor=download_log_rows())
    def test_linked_successors_do_not_leak_text_either(
        self, origin: dict[str, object], successor: dict[str, object],
    ) -> None:
        successor["source_download_log_id"] = origin["id"]
        successor["id"] = int(str(origin["id"])) + 100000
        target = ClassifyRenderTarget()
        target.prepare([origin, successor])
        rendered = target.render(origin)
        check_every_watched_field_rendered(rendered, self.WATCHED)


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


class TestDerivationCheckersTripOnViolations(unittest.TestCase):
    """The derivation checkers detect a fail-open watched set."""

    SHAPES = {
        "text": Shape(str, False),
        "basis": Shape(dict[str, object], False),
        "number": Shape(int, True),
    }

    def test_the_honest_partition_passes(self) -> None:
        check_derivation_matches_the_shapes(
            self.SHAPES, ("text", "basis"), ("number",))

    def test_a_text_field_left_unwatched_is_caught(self) -> None:
        with self.assertRaises(AssertionError):
            check_derivation_matches_the_shapes(
                self.SHAPES, ("text",), ("basis", "number"))

    def test_a_numeric_field_dragged_into_the_watched_set_is_caught(
        self,
    ) -> None:
        with self.assertRaises(AssertionError):
            check_derivation_matches_the_shapes(
                self.SHAPES, ("text", "basis", "number"), ())

    def test_a_lost_field_is_caught(self) -> None:
        with self.assertRaises(AssertionError):
            check_derivation_matches_the_shapes(
                self.SHAPES, ("text", "basis"), ())

    def test_text_in_an_unwatched_field_is_caught(self) -> None:
        with self.assertRaises(AssertionError):
            check_no_unwatched_text({"n": "not a number"}, ("n",))
        check_no_unwatched_text({"n": 320}, ("n",))

    def test_a_rendered_row_missing_a_watched_field_is_caught(self) -> None:
        with self.assertRaises(AssertionError):
            check_every_watched_field_rendered(
                RenderedRow(id=1, fields={"verdict": "v"}),
                ("verdict", "summary"))


if __name__ == "__main__":
    unittest.main()
