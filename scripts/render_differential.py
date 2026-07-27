"""Live-corpus differential for operator-facing derived text.

Two modes, deliberately separate so the tool never does git surgery and
stays directly testable:

* ``render`` reads a corpus JSONL (one DB row object per line), applies a
  render target, and writes a rendered JSONL: one object per line carrying
  the row's id and every watched output field.
* ``diff`` reads two rendered JSONL files and reports total rows, changed
  rows, and the changed-row count for EVERY field, plus a bounded sample of
  concrete before/after pairs per field.

The two-render dance lives in the runbook, not in this tool: check the base
ref out into a ``git worktree``, run ``render`` there, run ``render`` in the
working tree, then ``diff`` the two outputs. Full recipe in
``.claude/rules/test-fidelity.md`` under "Rule D".

Two rules keep the differential from making the very kind of fluent-but-wrong
claim it exists to catch:

* **The watched field set is derived and fails CLOSED.** A field is watched
  unless its declared type is PROVABLY non-text. Nested Structs, ``Any``,
  and ``object``-valued containers are watched, not skipped — an earlier
  fail-open version silently left ``comparison_basis`` (a
  ``dict[str, object]`` carrying eight operator-visible strings) unwatched,
  so deleting the whole "Compared" evidence row from every card rendered as
  zero changes.
* **Nothing text-bearing may escape that set.** Every render checks the
  unwatched fields for text at runtime and fails closed if any holds a
  string. The derivation and its converse are checked in both directions.
"""

from __future__ import annotations

import argparse
import contextlib
import importlib
import os
import sys
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from typing import Protocol, runtime_checkable

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

import msgspec
import msgspec.inspect

from lib.json_narrow import is_object_list, is_str_object_dict
from lib.pipeline_db.download_log import _DownloadLogMixin
from web.classify import ClassifiedEntry
from web.download_history_view import build_recents_download_log_rows

DEFAULT_SAMPLES_PER_FIELD = 3


class RenderDifferentialError(Exception):
    """A corpus, rendered file, or target violated the harness contract."""


class RenderedRow(msgspec.Struct, frozen=True):
    """One corpus row's rendered output, keyed by output field name.

    Values are whatever JSON the renderer produced for a watched field —
    strings, lists of strings, and the nested objects a field like
    ``comparison_basis`` carries.
    """

    id: int
    fields: dict[str, object]


class FieldChange(msgspec.Struct, frozen=True):
    """One concrete before/after pair, sampled for the report."""

    id: int
    field: str
    base: object
    current: object


class DiffReport(msgspec.Struct, frozen=True):
    """The differential: totals, per-field change counts, bounded samples.

    ``changed_by_field`` covers EVERY field compared, zeros included —
    "badge: 0" is the evidence that badge did not move. ``base_only_fields``
    and ``current_only_fields`` are empty unless the run was explicitly
    allowed to tolerate a changed output-field set, and name every field
    that could not be compared.
    """

    total_rows: int
    changed_rows: int
    changed_by_field: dict[str, int]
    samples: list[FieldChange]
    base_only_fields: list[str] = msgspec.field(default_factory=list[str])
    current_only_fields: list[str] = msgspec.field(default_factory=list[str])


@runtime_checkable
class RenderTarget(Protocol):
    """A render target: optionally observe the corpus, then render each row."""

    def prepare(self, rows: Iterable[Mapping[str, object]]) -> None:
        """Observe the whole corpus before rendering (cross-row projections)."""

    def render(self, row: Mapping[str, object]) -> RenderedRow:
        """Render one corpus row into its watched output fields."""
        ...


# ---------------------------------------------------------------------------
# Output-field derivation — fails closed
# ---------------------------------------------------------------------------

_NON_TEXT_LEAVES = (
    msgspec.inspect.IntType,
    msgspec.inspect.FloatType,
    msgspec.inspect.BoolType,
    msgspec.inspect.NoneType,
    msgspec.inspect.DateTimeType,
    msgspec.inspect.DateType,
    msgspec.inspect.TimeType,
    msgspec.inspect.TimeDeltaType,
    msgspec.inspect.UUIDType,
    msgspec.inspect.DecimalType,
)


def _provably_non_text(field_type: msgspec.inspect.Type) -> bool:
    """Whether a declared msgspec type can NEVER carry operator-visible text.

    Deliberately inverted relative to the obvious reading: only the leaf
    types listed above, and containers built exclusively from them, are
    provably text-free. Everything else — ``str``, a nested ``Struct``,
    ``Any``, ``object``, an enum, a type this function has never seen — is
    watched. Fail-open here is invisible under-reporting, which is the worst
    failure this tool can have.
    """
    if isinstance(field_type, _NON_TEXT_LEAVES):
        return True
    if isinstance(field_type, msgspec.inspect.UnionType):
        return all(_provably_non_text(member) for member in field_type.types)
    if isinstance(field_type, (msgspec.inspect.ListType,
                               msgspec.inspect.SetType,
                               msgspec.inspect.FrozenSetType,
                               msgspec.inspect.VarTupleType)):
        return _provably_non_text(field_type.item_type)
    if isinstance(field_type, msgspec.inspect.TupleType):
        return all(
            _provably_non_text(member) for member in field_type.item_types)
    if isinstance(field_type, msgspec.inspect.DictType):
        # A JSON object's keys are always strings, so a dict is text-free
        # only when its keys are not strings AND its values are text-free.
        return (
            _provably_non_text(field_type.key_type)
            and _provably_non_text(field_type.value_type)
        )
    return False


def _struct_fields(
    struct_type: type[msgspec.Struct],
) -> tuple[msgspec.inspect.Field, ...]:
    info = msgspec.inspect.type_info(struct_type)
    if not isinstance(info, msgspec.inspect.StructType):
        raise RenderDifferentialError(
            f"{struct_type.__name__} is not a msgspec Struct type")
    return tuple(info.fields)


def watched_field_names(
    struct_type: type[msgspec.Struct],
) -> tuple[str, ...]:
    """Every field of a render target's output Struct that may carry text.

    Derived from the declared types, in declaration order — the whole point
    of the differential is that nobody chooses which fields it watches.
    """
    return tuple(
        field.encode_name for field in _struct_fields(struct_type)
        if not _provably_non_text(field.type)
    )


def unwatched_field_names(
    struct_type: type[msgspec.Struct],
) -> tuple[str, ...]:
    """The complement of :func:`watched_field_names` on the same Struct."""
    return tuple(
        field.encode_name for field in _struct_fields(struct_type)
        if _provably_non_text(field.type)
    )


def contains_text(value: object) -> bool:
    """Whether an already-rendered JSON value exposes operator-visible text.

    A string is text; so is any non-empty mapping, because a JSON object's
    keys are themselves rendered text. An empty container is not.
    """
    if isinstance(value, str):
        return True
    if is_object_list(value):
        return any(contains_text(item) for item in value)
    if is_str_object_dict(value):
        return bool(value)
    return False


def _json_plain(field: str, value: object) -> object:
    """Validate one rendered value is plain JSON, failing closed otherwise."""
    if value is None or isinstance(value, (str, bool, int, float)):
        return value
    if is_object_list(value):
        return [_json_plain(field, item) for item in value]
    if is_str_object_dict(value):
        return {key: _json_plain(field, item) for key, item in value.items()}
    raise RenderDifferentialError(
        f"field {field!r} rendered a non-JSON value: {type(value).__name__}")


def project_output_fields(
    item: Mapping[str, object],
    watched: Sequence[str],
    unwatched: Sequence[str],
) -> dict[str, object]:
    """Project a rendered item onto its watched fields, converse-checked.

    The converse is the half that matters: if any field the derivation
    decided to IGNORE holds text at runtime, the derivation is wrong and
    this render fails rather than under-reporting.
    """
    for name in unwatched:
        if name in item and contains_text(item[name]):
            raise RenderDifferentialError(
                f"unwatched field {name!r} rendered text ({item[name]!r}): "
                "the watched-field derivation missed operator-facing copy")
    missing = [name for name in watched if name not in item]
    if missing:
        raise RenderDifferentialError(
            f"rendered item is missing watched fields: {missing}")
    return {name: _json_plain(name, item[name]) for name in watched}


# ---------------------------------------------------------------------------
# Render targets
# ---------------------------------------------------------------------------

_CLASSIFIED_WATCHED = watched_field_names(ClassifiedEntry)
_CLASSIFIED_UNWATCHED = unwatched_field_names(ClassifiedEntry)


class ClassifyRenderTarget:
    """Render a ``download_log`` row through the whole Recents render path.

    The differential protocol renders one row at a time, but Recents owns
    cross-row linked-import projection. ``prepare`` therefore renders the
    whole corpus once through the production batch owner and ``render`` only
    selects the prepared item.
    """

    def __init__(self) -> None:
        self._items_by_id: dict[int, dict[str, object]] = {}

    def prepare(self, rows: Iterable[Mapping[str, object]]) -> None:
        overlaid = [
            _DownloadLogMixin._overlay_evidence_onto_download_log_row(
                dict(row)
            )
            for row in rows
        ]
        items = build_recents_download_log_rows(
            overlaid,
            linked_successor_rows=overlaid,
        )
        self._items_by_id = {
            item_id: item
            for item in items
            for item_id in [item.get("id")]
            if isinstance(item_id, int) and not isinstance(item_id, bool)
        }

    def render(self, row: Mapping[str, object]) -> RenderedRow:
        row_id = row.get("id")
        if not isinstance(row_id, int) or isinstance(row_id, bool):
            raise RenderDifferentialError(
                f"corpus row has no integer id: {row_id!r}")
        item = self._items_by_id.get(row_id)
        if item is None:
            raise RenderDifferentialError(
                f"corpus row id {row_id} was not prepared")
        return RenderedRow(
            id=row_id,
            fields=project_output_fields(
                item, _CLASSIFIED_WATCHED, _CLASSIFIED_UNWATCHED),
        )


class _CallableTarget:
    """Adapter so ``--target module:function`` can stay a plain function."""

    def __init__(
        self, spec: str, render_one: Callable[..., object],
    ) -> None:
        self._spec = spec
        self._render_one = render_one

    def prepare(self, rows: Iterable[Mapping[str, object]]) -> None:
        """A plain function target has no cross-row state to build."""

    def render(self, row: Mapping[str, object]) -> RenderedRow:
        rendered = self._render_one(row)
        try:
            return msgspec.convert(
                msgspec.to_builtins(rendered),
                type=RenderedRow,
                strict=True,
            )
        except (TypeError, msgspec.ValidationError) as exc:
            raise RenderDifferentialError(
                f"render target {self._spec!r} returned "
                f"{type(rendered).__name__}, expected RenderedRow wire shape"
            ) from exc


DEFAULT_TARGET_SPEC = "scripts.render_differential:ClassifyRenderTarget"


def load_render_target(spec: str | None) -> RenderTarget:
    """Resolve a ``module:attribute`` render target, or the classify default.

    The default is constructed from the module-local class rather than
    imported by dotted path: this file is normally executed as a script, so
    importing ``scripts.render_differential`` would load a second copy of
    this module under a different name.

    The attribute may be a ``RenderTarget`` class, a ready ``RenderTarget``
    instance, or a plain ``row -> RenderedRow`` function.
    """
    if spec is None or spec == DEFAULT_TARGET_SPEC:
        return ClassifyRenderTarget()
    module_name, _, attribute = spec.partition(":")
    if not module_name or not attribute:
        raise RenderDifferentialError(
            f"render target must be 'module:attribute', got {spec!r}")
    try:
        module = importlib.import_module(module_name)
    except ImportError as exc:
        raise RenderDifferentialError(
            f"render target module {module_name!r} is not importable: {exc}"
        ) from exc
    resolved = getattr(module, attribute, None)
    if resolved is None:
        raise RenderDifferentialError(
            f"render target {spec!r} does not exist")
    if isinstance(resolved, type):
        resolved = resolved()
    if isinstance(resolved, RenderTarget):
        return resolved
    if callable(resolved):
        return _CallableTarget(spec, resolved)
    raise RenderDifferentialError(
        f"render target {spec!r} is neither a RenderTarget nor a callable")


# ---------------------------------------------------------------------------
# Diff summary (pure)
# ---------------------------------------------------------------------------

def _index_by_id(
    rows: Sequence[RenderedRow], side: str,
) -> dict[int, RenderedRow]:
    indexed: dict[int, RenderedRow] = {}
    for row in rows:
        if row.id in indexed:
            raise RenderDifferentialError(
                f"{side} corpus repeats row id {row.id}")
        indexed[row.id] = row
    return indexed


def _sample_ids(ids: Iterable[int], limit: int = 5) -> str:
    listed = sorted(ids)
    head = ", ".join(str(value) for value in listed[:limit])
    if len(listed) > limit:
        head += f", … ({len(listed)} total)"
    return head


def _side_field_set(
    rows: Sequence[RenderedRow], side: str,
) -> frozenset[str]:
    """The one field set every row on a side must carry."""
    expected: frozenset[str] | None = None
    for row in rows:
        names = frozenset(row.fields)
        if expected is None:
            expected = names
        elif names != expected:
            raise RenderDifferentialError(
                f"{side} corpus row {row.id} has a different field set: "
                f"missing {sorted(expected - names)}, "
                f"extra {sorted(names - expected)}")
    return expected if expected is not None else frozenset()


def summarize_render_diff(
    base: Sequence[RenderedRow],
    current: Sequence[RenderedRow],
    *,
    samples_per_field: int = DEFAULT_SAMPLES_PER_FIELD,
    allow_field_drift: bool = False,
) -> DiffReport:
    """Compare two rendered corpora, field by field, over the same row ids.

    Fails closed on every shape that could hide a regression: a row present
    on one side only, a repeated id, or a field set that drifted between
    the two renders. Skipping any of those silently is exactly how a
    changed row goes unreported.

    ``allow_field_drift`` is the one sanctioned relaxation, for the PR shape
    that adds or removes an output field: the shared fields are compared and
    the unshared ones are named in the report, never dropped quietly.
    """
    if samples_per_field < 0:
        raise ValueError("samples_per_field must be non-negative")
    base_by_id = _index_by_id(base, "base")
    current_by_id = _index_by_id(current, "current")
    missing = set(base_by_id) - set(current_by_id)
    extra = set(current_by_id) - set(base_by_id)
    if missing or extra:
        raise RenderDifferentialError(
            "rendered corpora cover different rows: "
            f"missing from current [{_sample_ids(missing)}], "
            f"absent from base [{_sample_ids(extra)}]")
    base_fields = _side_field_set(base, "base")
    current_fields = _side_field_set(current, "current")
    base_only = sorted(base_fields - current_fields)
    current_only = sorted(current_fields - base_fields)
    if (base_only or current_only) and not allow_field_drift:
        raise RenderDifferentialError(
            "rendered field sets differ between base and current: "
            f"base-only {base_only}, current-only {current_only}. "
            "Re-run with --allow-field-drift to compare the shared fields "
            "and have the unshared ones named in the report.")
    field_names = tuple(sorted(base_fields & current_fields))

    counts = {name: 0 for name in field_names}
    sampled = {name: 0 for name in field_names}
    samples: list[FieldChange] = []
    changed_rows = 0
    for row_id in sorted(base_by_id):
        base_row = base_by_id[row_id].fields
        current_row = current_by_id[row_id].fields
        row_changed = False
        for name in field_names:
            if base_row[name] == current_row[name]:
                continue
            row_changed = True
            counts[name] += 1
            if sampled[name] < samples_per_field:
                sampled[name] += 1
                samples.append(FieldChange(
                    id=row_id,
                    field=name,
                    base=base_row[name],
                    current=current_row[name],
                ))
        if row_changed:
            changed_rows += 1
    samples.sort(key=lambda sample: (sample.field, sample.id))
    return DiffReport(
        total_rows=len(base_by_id),
        changed_rows=changed_rows,
        changed_by_field=dict(sorted(counts.items())),
        samples=samples,
        base_only_fields=base_only,
        current_only_fields=current_only,
    )


def format_report(report: DiffReport) -> str:
    """Render the differential as the operator-readable summary."""
    lines = [
        f"rows: {report.total_rows}",
        f"changed rows: {report.changed_rows}",
    ]
    if report.base_only_fields or report.current_only_fields:
        lines.append(
            f"NOT COMPARED — base only: {report.base_only_fields}, "
            f"current only: {report.current_only_fields}")
    lines.append("changed rows by field:")
    width = max((len(name) for name in report.changed_by_field), default=0)
    for name, count in report.changed_by_field.items():
        lines.append(f"  {name.ljust(width)}  {count}")
    if report.samples:
        lines.append("samples:")
        for sample in report.samples:
            lines.append(f"  [{sample.field}] id={sample.id}")
            lines.append(f"    base:    {sample.base!r}")
            lines.append(f"    current: {sample.current!r}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# I/O
# ---------------------------------------------------------------------------

def _corpus_rows(path: str) -> Iterator[dict[str, object]]:
    """Stream a corpus JSONL as typed row dicts.

    This is a real wire boundary, so the line is DECODED into
    ``dict[str, object]`` rather than narrowed after the fact.
    """
    with open(path, encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            try:
                yield msgspec.json.decode(line, type=dict[str, object])
            except (msgspec.DecodeError, msgspec.ValidationError) as exc:
                raise RenderDifferentialError(
                    f"{path}:{line_number}: corpus line is not a JSON object: "
                    f"{exc}") from exc


def read_rendered(path: str) -> list[RenderedRow]:
    """Decode a rendered JSONL file into typed rows."""
    rows: list[RenderedRow] = []
    with open(path, encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            try:
                rows.append(msgspec.json.decode(line, type=RenderedRow))
            except (msgspec.DecodeError, msgspec.ValidationError) as exc:
                raise RenderDifferentialError(
                    f"{path}:{line_number}: not a rendered row: {exc}"
                ) from exc
    return rows


def render_corpus(
    corpus_path: str, out_path: str | None, target: RenderTarget,
) -> int:
    """Render every corpus row.

    Two passes over the file: the target observes the whole corpus first
    (cross-row projections need that), then rows stream through the
    renderer so a large corpus stays memory-bounded.
    """
    target.prepare(_corpus_rows(corpus_path))
    count = 0
    output = (
        contextlib.nullcontext(sys.stdout)
        if out_path is None
        else open(out_path, "w", encoding="utf-8")  # noqa: SIM115 - managed below
    )
    with output as handle:
        for row in _corpus_rows(corpus_path):
            handle.write(msgspec.json.encode(target.render(row)).decode())
            handle.write("\n")
            count += 1
    return count


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _non_negative_int(raw: str) -> int:
    value = int(raw)
    if value < 0:
        raise argparse.ArgumentTypeError("must be non-negative")
    return value


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="render_differential.py",
        description=(
            "Differential a corpus of DB rows through a render target, so a "
            "copy change is measured against real rows instead of asserted."),
    )
    sub = parser.add_subparsers(dest="mode", required=True)

    render = sub.add_parser(
        "render", help="Render a corpus JSONL through a render target")
    render.add_argument(
        "--corpus", required=True,
        help="Corpus JSONL: one DB row object per line")
    render.add_argument(
        "--out", default=None,
        help="Rendered JSONL output path (default: stdout)")
    render.add_argument(
        "--target", default=None,
        help=f"Render target as module:attribute (default: {DEFAULT_TARGET_SPEC})")

    diff = sub.add_parser(
        "diff", help="Compare two rendered JSONL files field by field")
    diff.add_argument("--base", required=True, help="Base rendered JSONL")
    diff.add_argument("--current", required=True, help="Current rendered JSONL")
    diff.add_argument(
        "--samples", type=_non_negative_int, default=DEFAULT_SAMPLES_PER_FIELD,
        help="Concrete before/after pairs to show per changed field")
    diff.add_argument(
        "--allow-field-drift", action="store_true",
        help=("Compare the shared fields when the output field set changed, "
              "naming the unshared fields in the report"))
    diff.add_argument(
        "--json", action="store_true", help="Print the report as JSON")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        if args.mode == "render":
            count = render_corpus(
                args.corpus, args.out, load_render_target(args.target))
            print(f"rendered {count} rows", file=sys.stderr)
            return 0
        report = summarize_render_diff(
            read_rendered(args.base),
            read_rendered(args.current),
            samples_per_field=args.samples,
            allow_field_drift=args.allow_field_drift,
        )
        if args.json:
            print(msgspec.json.encode(report).decode())
        else:
            print(format_report(report))
        return 0
    except (RenderDifferentialError, OSError) as exc:
        print(f"render-differential: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
