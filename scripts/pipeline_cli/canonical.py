"""pipeline-cli ``canonical`` commands (#1059).

The operator half of the MusicBrainz merge reconciler. Counterpart of
``POST /api/canonical/reconcile``, ``POST /api/canonical/retire``, and
``GET /api/canonical``;
both surfaces are thin adapters over ``CanonicalReleaseService``, which is
the one canonical execution path.

Exit codes follow the house convention: ``0`` success, ``2`` not found,
``4`` wrong state (a superseded row), ``5`` an unusable stored identity.
"""

from __future__ import annotations

import argparse
import json
from typing import Protocol

from lib.canonical_release_service import (
    OUTCOME_FROZEN,
    OUTCOME_INVALID_IDENTITY,
    OUTCOME_NO_CANONICAL,
    OUTCOME_NOT_FOUND,
    OUTCOME_STALE,
    CanonicalReconcileResult,
    CanonicalReleaseDB,
    CanonicalReleaseService,
    CanonicalRetireResult,
    configure_reconciliation_mirror,
)
from lib.config import read_runtime_config
from lib.mb_canonical import canonical_release_id
from scripts.pipeline_cli._format import _json_default

#: Module-local DI seam for the argparse dispatcher (``code-quality.md``
#: § MOCKS, strategy 3), matching ``web.routes.canonical``. The command
#: constructs the service itself, so a test has no kwarg to inject through.
#: The MusicBrainz WS/2 call behind it is an external HTTP edge.
canonical_release_fn = canonical_release_id


class _CanonicalDB(CanonicalReleaseDB, Protocol):
    """The service's DB surface, which is all this command touches."""


#: outcome -> exit code. Every other outcome is a successful observation:
#: "no merge declared" and "already current" are answers, not failures.
_EXIT_CODES = {
    OUTCOME_NOT_FOUND: 2,
    OUTCOME_FROZEN: 4,
    OUTCOME_NO_CANONICAL: 4,
    OUTCOME_STALE: 4,
    OUTCOME_INVALID_IDENTITY: 5,
}


def _emit(payload: dict[str, object]) -> None:
    print(json.dumps(payload, indent=2, default=_json_default))


def _result_payload(result: CanonicalReconcileResult) -> dict[str, object]:
    return {
        "request_id": result.request_id,
        "outcome": result.outcome,
        "acquisition_release_id": result.acquisition_release_id,
        "canonical_release_id": result.canonical_release_id,
        "previous_canonical_release_id": result.previous_canonical_release_id,
        "changed": result.changed,
    }


def _retire_payload(result: CanonicalRetireResult) -> dict[str, object]:
    return {
        "request_id": result.request_id,
        "outcome": result.outcome,
        "canonical_release_id": None,
        "previous_canonical_release_id": result.previous_canonical_release_id,
        "changed": result.changed,
    }


def cmd_canonical(db: _CanonicalDB, args: argparse.Namespace) -> int:
    """Dispatch ``canonical reconcile`` / ``retire`` / ``show``."""
    if args.canonical_command == "show":
        row = db.get_request(args.id)
        if row is None:
            _emit({"request_id": args.id, "outcome": OUTCOME_NOT_FOUND})
            return 2
        _emit({
            "request_id": args.id,
            "status": row.get("status"),
            "mb_release_id": row.get("mb_release_id"),
            "discogs_release_id": row.get("discogs_release_id"),
            "canonical_release_id": row.get("canonical_release_id"),
            "canonical_resolved_at": row.get("canonical_resolved_at"),
        })
        return 0

    if args.canonical_command == "retire":
        result = CanonicalReleaseService(db).retire_request(args.id)
        _emit(_retire_payload(result))
        return _EXIT_CODES.get(result.outcome, 0)

    # lib/mb_canonical is inert until a process wires a base. A surface that
    # forgets does not fail loudly — it reports no_redirect for every row and
    # exits 0, which reads as "the library is already correct".
    configure_reconciliation_mirror(read_runtime_config().musicbrainz_api_base)

    service = CanonicalReleaseService(db, canonical_fn=canonical_release_fn)

    if args.id is not None:
        result = service.reconcile_request(args.id)
        _emit(_result_payload(result))
        return _EXIT_CODES.get(result.outcome, 0)

    # Whole-library sweep. Streamed so a ten-minute run is observable while
    # it runs rather than only at the end.
    def _log(result: CanonicalReconcileResult) -> None:
        if result.changed:
            print(
                f"resolved request {result.request_id}: "
                f"{result.acquisition_release_id} -> "
                f"{result.canonical_release_id}",
                flush=True,
            )

    sweep = service.reconcile_all(on_result=_log)
    _emit({
        "scanned": sweep.scanned,
        "changed": sweep.changed,
        "outcome_counts": dict(sweep.outcome_counts),
        "resolved": [_result_payload(r) for r in sweep.resolved],
    })
    return 0


def add_canonical_subparser(
    sub: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    parser = sub.add_parser(
        "canonical",
        help="Inspect or reconcile MusicBrainz merge survivors (#1059)",
    )
    canonical_sub = parser.add_subparsers(
        dest="canonical_command", required=True,
    )

    reconcile = canonical_sub.add_parser(
        "reconcile",
        help=(
            "Ask MusicBrainz what each release is called now and store any "
            "declared merge survivor. Omit --id to sweep the whole library."
        ),
    )
    reconcile.add_argument(
        "--id",
        type=int,
        default=None,
        help="Reconcile one request instead of the whole library.",
    )

    show = canonical_sub.add_parser(
        "show",
        help="Show one request's stored acquisition and survivor ids.",
    )
    show.add_argument("id", type=int, help="Request id.")

    retire = canonical_sub.add_parser(
        "retire",
        help="Explicitly retire one stored MusicBrainz merge survivor.",
    )
    retire.add_argument("--id", type=int, required=True, help="Request id.")
    retire.add_argument(
        "--confirm", choices=("RETIRE",), required=True,
        help="Required acknowledgement for this explicit identity action.",
    )
