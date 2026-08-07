"""MusicBrainz merge-survivor routes (#1059).

Thin adapters over ``CanonicalReleaseService`` — the same service
``pipeline-cli canonical`` wraps (CLI ⇄ API symmetry). No logic here, no
direct-DB fallback, and the outcome→status mapping mirrors the CLI's
outcome→exit-code mapping exactly.
"""

import logging
from typing import Literal

from pydantic import BaseModel, Field

from lib.canonical_release_service import (
    OUTCOME_FROZEN,
    OUTCOME_INVALID_IDENTITY,
    OUTCOME_NO_CANONICAL,
    OUTCOME_NOT_FOUND,
    OUTCOME_STALE,
    CanonicalReconcileResult,
    CanonicalReleaseService,
    CanonicalRetireResult,
    configure_reconciliation_mirror,
)
from lib.config import read_runtime_config
from lib.mb_canonical import canonical_release_id
from web.routes._pydantic import parse_body
from web.routes._registry import RouteHandler, RouteRegistration, route
from web.routes._server_access import _server

log = logging.getLogger(__name__)

#: Module-local DI seam for the URL dispatcher (``code-quality.md`` § MOCKS,
#: strategy 3). The route constructs the service itself, so a test has no
#: kwarg to inject through; it replaces this binding instead. The MusicBrainz
#: WS/2 call behind it is an external HTTP edge.
canonical_release_fn = canonical_release_id

#: outcome -> HTTP status, matched to the CLI's exit codes:
#: 404/2 not found, 409/4 wrong state, 422/5 semantic violation.
_STATUS = {
    OUTCOME_NOT_FOUND: 404,
    OUTCOME_FROZEN: 409,
    OUTCOME_NO_CANONICAL: 409,
    OUTCOME_STALE: 409,
    OUTCOME_INVALID_IDENTITY: 422,
}


def _payload(result: CanonicalReconcileResult) -> dict[str, object]:
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


def get_canonical_request(
    h: RouteHandler, params: dict[str, list[str]],
) -> None:
    """``GET /api/canonical?id=<request_id>`` — stored merge state.

    Reports the frozen acquisition id alongside any survivor MusicBrainz
    has declared, so an operator can see the drift without inferring it
    from a missing album.
    """
    raw = params.get("id", [None])[0]
    if raw is None or raw == "":
        h._error("id is required")
        return
    try:
        request_id = int(raw)
    except (TypeError, ValueError):
        h._error("id must be an integer")
        return

    row = _server()._db().get_request(request_id)
    if row is None:
        h._json({"error": "Not found", "id": request_id}, status=404)
        return
    h._json({
        "request_id": int(row["id"]),
        "status": row.get("status"),
        "mb_release_id": row.get("mb_release_id"),
        "discogs_release_id": row.get("discogs_release_id"),
        "canonical_release_id": row.get("canonical_release_id"),
        "canonical_resolved_at": row.get("canonical_resolved_at"),
    })


class CanonicalReconcileRequest(BaseModel):
    """Body for ``POST /api/canonical/reconcile``."""

    request_id: int | None = None


class CanonicalRetireRequest(BaseModel):
    """Body for ``POST /api/canonical/retire``."""

    request_id: int = Field(gt=0, strict=True)
    confirm: Literal["RETIRE"]


def post_canonical_reconcile(h: RouteHandler, body: bytes) -> None:
    """``POST /api/canonical/reconcile`` — ask MusicBrainz, store the answer.

    With ``request_id`` reconciles that one row; without it sweeps every
    non-``replaced`` request, which is what the daily oneshot does. The
    sweep is the expensive call (~10 minutes over the live library), so the
    route exists for operator-initiated runs, not for the UI.
    """
    parsed = parse_body(h, body, CanonicalReconcileRequest)
    if parsed is None:
        return

    # Same inertness trap as the CLI: lib/mb_canonical is inert until a
    # process wires a base, and a surface that forgets reports no_redirect
    # for every row and returns 200 — which reads as "already correct".
    configure_reconciliation_mirror(read_runtime_config().musicbrainz_api_base)

    service = CanonicalReleaseService(
        _server()._db(), canonical_fn=canonical_release_fn,
    )
    if parsed.request_id is not None:
        result = service.reconcile_request(parsed.request_id)
        h._json(_payload(result), status=_STATUS.get(result.outcome, 200))
        return

    sweep = service.reconcile_all()
    h._json({
        "scanned": sweep.scanned,
        "changed": sweep.changed,
        "outcome_counts": dict(sweep.outcome_counts),
        "resolved": [_payload(r) for r in sweep.resolved],
    })


def post_canonical_retire(h: RouteHandler, body: bytes) -> None:
    """``POST /api/canonical/retire`` — explicitly clear one survivor."""
    parsed = parse_body(h, body, CanonicalRetireRequest)
    if parsed is None:
        return
    result = CanonicalReleaseService(_server()._db()).retire_request(
        parsed.request_id,
    )
    h._json(_retire_payload(result), status=_STATUS.get(result.outcome, 200))


ROUTES: list[RouteRegistration] = [
    route(
        "GET", "/api/canonical", get_canonical_request,
        "Stored MusicBrainz merge state for one request — the frozen "
        "acquisition release id plus any survivor an observed 301 has "
        "declared, and when it was observed.",
        classified=True,
    ),
    route(
        "POST", "/api/canonical/reconcile", post_canonical_reconcile,
        "Reconcile stored acquisition ids against MusicBrainz merge state. "
        "With request_id reconciles one row; without it sweeps every "
        "non-replaced request. Wraps CanonicalReleaseService, the same "
        "service pipeline-cli canonical wraps.",
        classified=True,
    ),
    route(
        "POST", "/api/canonical/retire", post_canonical_retire,
        "Explicitly retire one stored MusicBrainz merge survivor after the "
        "caller confirms RETIRE. The service fresh-reads and CAS-clears the "
        "survivor plus its observation; it never consults the mirror.",
        classified=True,
    ),
]
