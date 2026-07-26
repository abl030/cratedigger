#!/usr/bin/env python3
"""Live-corpus differential for operator-facing derived text.

Two modes, deliberately separate so the tool never does git surgery and
stays directly testable:

* ``render`` reads a corpus JSONL (one DB row object per line), applies a
  render target, and writes a rendered JSONL: one object per line carrying
  the row's id and every text-bearing field the target produces.
* ``diff`` reads two rendered JSONL files and reports total rows, changed
  rows, and the changed-row count for EVERY field, plus a bounded sample of
  concrete before/after pairs per field.

The two-render dance lives in the runbook, not in this tool: check the base
ref out into a ``git worktree``, run ``render`` there, run ``render`` in the
working tree, then ``diff`` the two outputs. Full recipe in
``.claude/rules/test-fidelity.md`` under "Rule D".

Why every field and not just the ones you expect to move: a differential
that only diffs the fields you already suspected is worthless. Proving the
other fields are byte-identical is most of the evidence, so the field set is
derived from the render target's output type by introspection and never
hand-listed.
"""

from __future__ import annotations

import argparse
import importlib
import os
import sys
from collections.abc import Callable, Iterable, Iterator, Sequence

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

import msgspec
import msgspec.inspect

from lib.json_narrow import is_object_list
from lib.pipeline_db.download_log import _DownloadLogMixin
from web.classify import ClassifiedEntry, LogEntry, classify_log_entry

DEFAULT_SAMPLES_PER_FIELD = 3

# One rendered field value. Text-bearing output is a string, a list of
# strings, or absent — anything else fails closed at render time rather
# than being silently stringified into a diff that cannot be read.
FieldValue = str | list[str] | None


class RenderDifferentialError(Exception):
    """A corpus, rendered file, or target violated the harness contract."""


class RenderedRow(msgspec.Struct, frozen=True):
    """One corpus row's rendered text, keyed by output field name."""

    id: int
    fields: dict[str, FieldValue]


class FieldChange(msgspec.Struct, frozen=True):
    """One concrete before/after pair, sampled for the report."""

    id: int
    field: str
    base: FieldValue
    current: FieldValue


class DiffReport(msgspec.Struct, frozen=True):
    """The differential: totals, per-field change counts, bounded samples.

    ``changed_by_field`` covers EVERY field in the rendered corpus, zeros
    included — "badge: 0" is the evidence that badge did not move.
    """

    total_rows: int
    changed_rows: int
    changed_by_field: dict[str, int]
    samples: list[FieldChange]


RenderTarget = Callable[[dict[str, object]], RenderedRow]


# ---------------------------------------------------------------------------
# Output-field derivation
# ---------------------------------------------------------------------------

def _mentions_str(field_type: msgspec.inspect.Type) -> bool:
    """Whether a declared msgspec type carries operator-visible text.

    Unions and sequence element types are followed. For mappings only the
    VALUE type is followed: a JSON object's keys are always strings, so
    recursing into ``key_type`` would call every dict field text-bearing.
    """
    if isinstance(field_type, msgspec.inspect.StrType):
        return True
    if isinstance(field_type, msgspec.inspect.UnionType):
        return any(_mentions_str(member) for member in field_type.types)
    if isinstance(field_type, msgspec.inspect.ListType):
        return _mentions_str(field_type.item_type)
    if isinstance(field_type, msgspec.inspect.SetType):
        return _mentions_str(field_type.item_type)
    if isinstance(field_type, msgspec.inspect.VarTupleType):
        return _mentions_str(field_type.item_type)
    if isinstance(field_type, msgspec.inspect.TupleType):
        return any(_mentions_str(member) for member in field_type.item_types)
    if isinstance(field_type, msgspec.inspect.DictType):
        return _mentions_str(field_type.value_type)
    return False


def text_bearing_field_names(
    struct_type: type[msgspec.Struct],
) -> tuple[str, ...]:
    """Every text-bearing field of a render target's output Struct.

    Derived from the declared type, in declaration order — the whole point
    of the differential is that nobody chooses which fields it watches.
    """
    info = msgspec.inspect.type_info(struct_type)
    if not isinstance(info, msgspec.inspect.StructType):
        raise RenderDifferentialError(
            f"{struct_type.__name__} is not a msgspec Struct type")
    return tuple(
        field.encode_name for field in info.fields if _mentions_str(field.type)
    )


def _text_value(field: str, value: object) -> FieldValue:
    """Narrow one rendered value, failing closed on anything untextual."""
    if value is None or isinstance(value, str):
        return value
    if is_object_list(value):
        items: list[str] = []
        for item in value:
            if not isinstance(item, str):
                raise RenderDifferentialError(
                    f"field {field!r} rendered a non-text list item: "
                    f"{type(item).__name__}")
            items.append(item)
        return items
    raise RenderDifferentialError(
        f"field {field!r} rendered a non-text value: {type(value).__name__}")


def rendered_fields(
    output: msgspec.Struct, names: Sequence[str],
) -> dict[str, FieldValue]:
    """Project a render target's output Struct onto its text fields."""
    payload: dict[str, object] = msgspec.to_builtins(output)
    return {name: _text_value(name, payload[name]) for name in names}


# ---------------------------------------------------------------------------
# Render targets
# ---------------------------------------------------------------------------

_CLASSIFIED_TEXT_FIELDS = text_bearing_field_names(ClassifiedEntry)


def classify_render_target(row: dict[str, object]) -> RenderedRow:
    """Render one ``download_log`` row exactly as Recents renders it.

    The corpus rows come from the production read seam's SELECT, so the
    production evidence overlay runs here too — otherwise every row whose
    measurement lives on ``album_quality_evidence`` would be rendered from
    NULL denorm columns and the differential would under-cover the fields
    that matter most.
    """
    row_id = row.get("id")
    if not isinstance(row_id, int) or isinstance(row_id, bool):
        raise RenderDifferentialError(
            f"corpus row has no integer id: {row_id!r}")
    overlaid = _DownloadLogMixin._overlay_evidence_onto_download_log_row(
        dict(row))
    classified = classify_log_entry(LogEntry.from_row(overlaid))
    return RenderedRow(
        id=row_id,
        fields=rendered_fields(classified, _CLASSIFIED_TEXT_FIELDS),
    )


DEFAULT_TARGET_SPEC = "scripts.render_differential:classify_render_target"


def load_render_target(spec: str | None) -> RenderTarget:
    """Resolve a ``module:function`` render target, or the classify default.

    The default is returned as the module-local object rather than being
    imported by dotted path: this file is normally executed as a script, so
    importing ``scripts.render_differential`` would load a second copy of
    this module under a different name.
    """
    if spec is None or spec == DEFAULT_TARGET_SPEC:
        return classify_render_target
    module_name, _, attribute = spec.partition(":")
    if not module_name or not attribute:
        raise RenderDifferentialError(
            f"render target must be 'module:function', got {spec!r}")
    try:
        module = importlib.import_module(module_name)
    except ImportError as exc:
        raise RenderDifferentialError(
            f"render target module {module_name!r} is not importable: {exc}"
        ) from exc
    target = getattr(module, attribute, None)
    if target is None or not callable(target):
        raise RenderDifferentialError(
            f"render target {spec!r} is not a callable")

    def render_one(row: dict[str, object]) -> RenderedRow:
        rendered = target(row)
        if not isinstance(rendered, RenderedRow):
            raise RenderDifferentialError(
                f"render target {spec!r} returned "
                f"{type(rendered).__name__}, expected RenderedRow")
        return rendered

    return render_one


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
) -> frozenset[str] | None:
    """The one field set every row on a side must carry, or None if empty."""
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
    return expected


def _require_stable_field_set(
    base: Sequence[RenderedRow], current: Sequence[RenderedRow],
) -> tuple[str, ...]:
    base_fields = _side_field_set(base, "base")
    current_fields = _side_field_set(current, "current")
    if base_fields is None and current_fields is None:
        return ()
    if base_fields != current_fields:
        raise RenderDifferentialError(
            "rendered field sets differ between base and current: "
            f"base-only {sorted((base_fields or frozenset()) - (current_fields or frozenset()))}, "
            f"current-only {sorted((current_fields or frozenset()) - (base_fields or frozenset()))}")
    return tuple(sorted(base_fields or frozenset()))


def summarize_render_diff(
    base: Sequence[RenderedRow],
    current: Sequence[RenderedRow],
    *,
    samples_per_field: int = DEFAULT_SAMPLES_PER_FIELD,
) -> DiffReport:
    """Compare two rendered corpora, field by field, over the same row ids.

    Fails closed on every shape that could hide a regression: a row present
    on one side only, a repeated id, or a field set that drifted between
    the two renders. Skipping any of those silently is exactly how a
    changed row goes unreported.
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
    field_names = _require_stable_field_set(base, current)

    counts = {name: 0 for name in field_names}
    sampled = {name: 0 for name in field_names}
    samples: list[FieldChange] = []
    changed_rows = 0
    for row_id in sorted(base_by_id):
        base_fields = base_by_id[row_id].fields
        current_fields = current_by_id[row_id].fields
        row_changed = False
        for name in field_names:
            if base_fields[name] == current_fields[name]:
                continue
            row_changed = True
            counts[name] += 1
            if sampled[name] < samples_per_field:
                sampled[name] += 1
                samples.append(FieldChange(
                    id=row_id,
                    field=name,
                    base=base_fields[name],
                    current=current_fields[name],
                ))
        if row_changed:
            changed_rows += 1
    samples.sort(key=lambda sample: (sample.field, sample.id))
    return DiffReport(
        total_rows=len(base_by_id),
        changed_rows=changed_rows,
        changed_by_field=dict(sorted(counts.items())),
        samples=samples,
    )


def format_report(report: DiffReport) -> str:
    """Render the differential as the operator-readable summary."""
    lines = [
        f"rows: {report.total_rows}",
        f"changed rows: {report.changed_rows}",
        "changed rows by field:",
    ]
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
                    f"{path}:{line_number}: corpus line is not a JSON object: {exc}"
                ) from exc


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
                    f"{path}:{line_number}: not a rendered row: {exc}") from exc
    return rows


def render_corpus(
    corpus_path: str, out_path: str | None, target: RenderTarget,
) -> int:
    """Render every corpus row, streaming so a large corpus stays bounded."""
    handle = sys.stdout if out_path is None else open(
        out_path, "w", encoding="utf-8")
    count = 0
    try:
        for row in _corpus_rows(corpus_path):
            handle.write(msgspec.json.encode(target(row)).decode())
            handle.write("\n")
            count += 1
    finally:
        if out_path is not None:
            handle.close()
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
        help=f"Render target as module:function (default: {DEFAULT_TARGET_SPEC})")

    diff = sub.add_parser(
        "diff", help="Compare two rendered JSONL files field by field")
    diff.add_argument("--base", required=True, help="Base rendered JSONL")
    diff.add_argument("--current", required=True, help="Current rendered JSONL")
    diff.add_argument(
        "--samples", type=_non_negative_int, default=DEFAULT_SAMPLES_PER_FIELD,
        help="Concrete before/after pairs to show per changed field")
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
