"""Live-corpus differential for the QUALITY DECISION itself.

``scripts/render_differential.py`` measures what a copy change does to
operator-facing derived TEXT. This measures what a policy change does to
the DECISION: it re-decides real persisted ``album_quality_evidence``
rows through the real decider and reports changed rows by field.

The two are deliberately the same shape and share one diff engine
(:mod:`scripts.render_differential`'s ``summarize_render_diff``,
``RenderedRow``, ``format_report``), because the question is the same one
— "on the rows that actually exist, what does this change?" — and because
a second copy of that engine would be a parallel code path.

Two modes, same two-tree runbook as Rule D:

* ``decide`` reads a corpus JSONL (one ``album_quality_evidence`` row
  object per line, with its snapshot files under ``files``), runs
  ``full_pipeline_decision_from_evidence`` over each, and writes one
  decided JSONL row per corpus row.
* ``diff`` compares two decided JSONL files field by field.

    git archive <base-ref> | tar -x -C /tmp/dd-base
    cp scripts/decision_differential.py /tmp/dd-base/scripts/   # if newer
    nix-shell --run "python3 /tmp/dd-base/scripts/decision_differential.py \\
      decide --corpus /tmp/evidence.jsonl --out /tmp/base.jsonl"
    nix-shell --run "python3 scripts/decision_differential.py \\
      decide --corpus /tmp/evidence.jsonl --out /tmp/current.jsonl"
    nix-shell --run "python3 scripts/decision_differential.py diff \\
      --base /tmp/base.jsonl --current /tmp/current.jsonl"

Three properties keep the measurement honest, and each is the mirror of a
way a differential can lie:

* **The corpus is decoded by PRODUCTION'S decoder.** Rows go through
  ``PipelineDB._album_quality_evidence_from_row``, the same function the
  importer's reads use. A bespoke decoder here would measure a world the
  pipeline never sees — the test-fidelity.md Rule B failure, one layer up.
* **The watched field set is DERIVED from the decision dict, not chosen.**
  Every key the decider emits is compared, plus the two proof facts the
  dict does not carry (``verified_lossless_classifier`` and the
  ultrasonic leg's own outcome/reason), which is where a proof-gate
  change actually lands.
* **A row the decider refuses is recorded, never dropped.** An evidence
  row that is not action-ready raises in production too; silently
  skipping it would shrink the denominator and flatter the result.
"""

from __future__ import annotations

import argparse
import contextlib
import os
import sys
from collections.abc import Iterator, Mapping, Sequence

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

import msgspec

from lib.pipeline_db.evidence import _EvidenceMixin
from lib.quality import (
    AlbumQualityEvidence,
    UltrasonicProofLeg,
    full_pipeline_decision_from_evidence,
    is_preserved_source_spectral,
    mint_verified_lossless_proof,
    ultrasonic_proof_leg,
)
from lib.quality.pipeline import evidence_spectral_context
from scripts.render_differential import (
    DEFAULT_SAMPLES_PER_FIELD,
    RenderDifferentialError,
    RenderedRow,
    format_report,
    read_rendered,
    summarize_render_diff,
)

#: Decision-dict keys that are not decisions: the Stage-1 counterfactual
#: is audit-only reporting, and ``comparison_basis`` is a nested display
#: payload whose own differential is ``render_differential``'s job. They
#: are excluded by NAME rather than by type so adding a decision key can
#: never silently fall out of the watched set.
_AUDIT_ONLY_DECISION_KEYS: frozenset[str] = frozenset({
    "stage2_import_if_stage1_deferred",
    "comparison_basis_if_stage1_deferred",
})

#: Facts the decision dict does not carry but a proof-gate change moves.
#: ``verified_lossless_classifier`` is minted beside the decision, and the
#: leg's outcome/reason are the three-state fact the decision reduces to a
#: boolean. Reporting only the boolean would show a proof-gate change as
#: fewer moved rows than it really moved.
PROOF_FIELDS: tuple[str, ...] = (
    "verified_lossless_classifier",
    "ultrasonic_leg_outcome",
    "ultrasonic_leg_reason",
)

#: Recorded in place of a decision when the decider refuses the row.
DECISION_ERROR_FIELD = "decision_error"


def _decision_keys() -> tuple[str, ...]:
    """Every decision key, DERIVED from production's own dict contract.

    Taken from the flat simulator twin on a trivial world rather than
    hand-listed: the twins are contractually one dict shape (the parity
    property enforces it), so a new decision key joins the differential
    automatically. Deriving also keeps a REFUSED row comparable — it has
    no decision of its own, and a row carrying a different field set from
    its neighbours is a row the diff engine cannot compare at all.
    """
    from lib.quality import full_pipeline_decision

    shape = full_pipeline_decision(
        is_flac=False, min_bitrate=192, is_cbr=True,
    )
    return tuple(
        key for key in sorted(shape)
        if key not in _AUDIT_ONLY_DECISION_KEYS and key != "comparison_basis"
    )


DECISION_KEYS: tuple[str, ...] = _decision_keys()


def _evidence_from_corpus_row(
    row: Mapping[str, object],
) -> AlbumQualityEvidence:
    """Decode one corpus row with PRODUCTION's evidence decoder.

    ``files`` carries the snapshot rows the DB read joins in. The decoder
    is a ``@staticmethod`` for exactly this reason — the same shape
    ``render_differential`` uses for
    ``_overlay_evidence_onto_download_log_row``.
    """
    payload = dict(row)
    raw_files = payload.pop("files", None)
    file_rows: list[dict[str, object]] = []
    if isinstance(raw_files, list):
        for entry in raw_files:
            if not isinstance(entry, dict):
                raise RenderDifferentialError(
                    "corpus row 'files' must be a list of objects")
            file_rows.append(dict(entry))
    elif raw_files is not None:
        raise RenderDifferentialError(
            "corpus row 'files' must be a list of objects")
    measured_at = payload.get("measured_at")
    if isinstance(measured_at, str):
        # PG hands psycopg2 a datetime; a JSONL export hands us its ISO
        # text. Restore the production type rather than letting a string
        # ride into a Struct that declares datetime.
        payload["measured_at"] = _parse_timestamp(measured_at)
    try:
        return _EvidenceMixin._album_quality_evidence_from_row(
            payload, file_rows,
        )
    except (KeyError, TypeError, ValueError, msgspec.ValidationError) as exc:
        raise RenderDifferentialError(
            f"corpus row {payload.get('id')!r} is not an "
            f"album_quality_evidence row: {exc}") from exc


def _parse_timestamp(raw: str) -> object:
    from datetime import datetime

    try:
        return datetime.fromisoformat(raw)
    except ValueError as exc:
        raise RenderDifferentialError(
            f"measured_at is not an ISO timestamp: {raw!r}") from exc


def leg_for_evidence(evidence: AlbumQualityEvidence) -> UltrasonicProofLeg:
    """The ultrasonic proof leg the decider will build for this row.

    Built through the SAME context adapter the decider uses
    (``evidence_spectral_context``), so the differential cannot report a
    leg the decision never saw.
    """
    context = evidence_spectral_context(evidence)
    return ultrasonic_proof_leg(
        deficit_db=context.ultrasonic_deficit_db,
        spectral_measurement_version=context.spectral_measurement_version,
        decode_path=context.spectral_decode_path,
        preserved_source_spectral=is_preserved_source_spectral(
            context.spectral_subject, context.was_converted_from,
        ),
    )


def decide_row(row: Mapping[str, object]) -> RenderedRow:
    """Re-decide one corpus row through the real decider.

    The candidate is the row itself and there is no ``current``: the
    question this differential answers is "what does the policy change do
    to THIS album's own evidence", and pairing every row against a HAVE
    would measure the comparison rather than the change.
    """
    row_id = row.get("id")
    if not isinstance(row_id, int) or isinstance(row_id, bool):
        raise RenderDifferentialError(
            f"corpus row has no integer id: {row_id!r}")
    evidence = _evidence_from_corpus_row(row)
    fields: dict[str, object] = {}
    try:
        decision = full_pipeline_decision_from_evidence(evidence)
    except ValueError as exc:
        # A row the decider refuses is a real outcome and is compared as
        # one; dropping it would shrink the denominator. It still carries
        # the full key set, with no decision, so the diff engine can put
        # it beside its neighbours.
        fields[DECISION_ERROR_FIELD] = str(exc)
        decision = {}
    else:
        fields[DECISION_ERROR_FIELD] = None
        unknown = (
            set(decision)
            - set(DECISION_KEYS)
            - _AUDIT_ONLY_DECISION_KEYS
            - {"comparison_basis"}
        )
        if unknown:
            raise RenderDifferentialError(
                "the evidence decider emitted keys the flat twin does not: "
                f"{sorted(unknown)} — the dict-shape contract has drifted "
                "and this differential would silently not compare them")
    for key in DECISION_KEYS:
        value = decision.get(key)
        fields[key] = value if _is_json_scalar(value) else repr(value)
    leg = leg_for_evidence(evidence)
    proof = mint_verified_lossless_proof(
        bool(decision.get("verified_lossless")),
        was_converted_from=evidence.measurement.was_converted_from,
        detected_source_format=evidence.storage_format,
        spectral_grade=evidence.measurement.spectral_grade,
        ultrasonic_leg=leg,
    )
    fields["verified_lossless_classifier"] = (
        proof.classifier if proof is not None else None
    )
    fields["ultrasonic_leg_outcome"] = leg.outcome
    fields["ultrasonic_leg_reason"] = leg.reason
    return RenderedRow(id=row_id, fields=fields)


def _is_json_scalar(value: object) -> bool:
    return value is None or isinstance(value, (str, bool, int, float))


def _corpus_rows(path: str) -> Iterator[dict[str, object]]:
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


def decide_corpus(corpus_path: str, out_path: str | None) -> int:
    """Decide every corpus row, streaming so a large corpus stays bounded."""
    count = 0
    output = (
        contextlib.nullcontext(sys.stdout)
        if out_path is None
        else open(out_path, "w", encoding="utf-8")  # noqa: SIM115 - managed below
    )
    with output as handle:
        for row in _corpus_rows(corpus_path):
            handle.write(msgspec.json.encode(decide_row(row)).decode())
            handle.write("\n")
            count += 1
    return count


def _non_negative_int(raw: str) -> int:
    value = int(raw)
    if value < 0:
        raise argparse.ArgumentTypeError("must be non-negative")
    return value


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="decision_differential.py",
        description=(
            "Re-decide a corpus of album_quality_evidence rows through the "
            "real decider, so a policy change is measured against real rows "
            "instead of asserted."),
    )
    sub = parser.add_subparsers(dest="mode", required=True)

    decide = sub.add_parser(
        "decide", help="Decide a corpus JSONL through the real decider")
    decide.add_argument(
        "--corpus", required=True,
        help="Corpus JSONL: one album_quality_evidence row object per line")
    decide.add_argument(
        "--out", default=None,
        help="Decided JSONL output path (default: stdout)")

    diff = sub.add_parser(
        "diff", help="Compare two decided JSONL files field by field")
    diff.add_argument("--base", required=True, help="Base decided JSONL")
    diff.add_argument("--current", required=True, help="Current decided JSONL")
    diff.add_argument(
        "--samples", type=_non_negative_int, default=DEFAULT_SAMPLES_PER_FIELD,
        help="Concrete before/after pairs to show per changed field")
    diff.add_argument(
        "--allow-field-drift", action="store_true",
        help=("Compare the shared fields when the decision field set "
              "changed, naming the unshared fields in the report"))
    diff.add_argument(
        "--json", action="store_true", help="Print the report as JSON")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        if args.mode == "decide":
            count = decide_corpus(args.corpus, args.out)
            print(f"decided {count} rows", file=sys.stderr)
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
        print(f"decision-differential: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
