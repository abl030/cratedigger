"""Release-identity routes — resolve-rg lazy backfill and Replace.

Split from web/routes/pipeline.py (#522).
"""

import json

from pydantic import BaseModel, Field

from lib.release_identity import detect_release_source, normalize_release_id
from lib.replace_status import (
    RESOLVE_STATUS_LOOKUP_FAILED,
    RESOLVE_STATUS_MASTERLESS,
    RESOLVE_STATUS_MIRROR_UNCONFIGURED,
    RESOLVE_STATUS_MISSING_RELEASE_ID,
    RESOLVE_STATUS_NON_MB_RELEASE_ID,
    RESOLVE_STATUS_NO_RELEASE_GROUP,
    RESOLVE_STATUS_NOT_FOUND,
    RESOLVE_STATUS_RESOLVED,
    RESOLVE_STATUS_TRANSIENT,
)
from web import discogs as discogs_api
from web import mb as mb_api
from web.routes._pydantic import parse_body
from web.routes._registry import RouteRegistration, pattern_route
from web.routes._server_access import _server


def post_pipeline_resolve_rg(h, body: dict, req_id_str: str) -> None:
    """``POST /api/pipeline/<id>/resolve-rg``.

    Lazy-backfill ``album_requests.mb_release_group_id`` for a single
    legacy row that was added before the RG field was populated.

    Used by ``web/js/replace_picker.js`` standard-mode when the row has
    a null RG — the picker calls this endpoint, persists the resolved
    RG back to the row, then continues into the sibling fetch.

    The persisted side-effect is intentionally idempotent: if the row
    already has a non-null RG the route returns it untouched (no
    redundant MB hit because ``get_release(fresh=False)`` is cache-served).

    MB rows resolve the release group via the MB mirror. Discogs rows
    (numeric ``mb_release_id``) resolve the release's Discogs master
    instead — the release-group analog (KTD-1) — and persist it into
    the same ``mb_release_group_id`` column via the same
    ``update_request_fields`` call the MB branch uses.

    Status-code mapping:
      * 200 — ``status='resolved'`` (RG/master found; row updated or
              already set) or ``status='masterless'`` (Discogs release
              has no master; row left untouched — R2, the picker
              renders the one-element "nothing to swap to" state
              instead of an error)
      * 404 — request id does not exist
      * 422 — MB lookup returned no release_group_id (the upstream MB
              release has no RG attached)
      * 503 — transient mirror error (timeout, network, malformed
              JSON) — retryable — or ``status='mirror_unconfigured'``
              when the Discogs mirror isn't configured (R11)
    """
    try:
        request_id = int(req_id_str)
    except (TypeError, ValueError):
        h._error("Invalid request id")
        return

    db = _server()._db()
    row = db.get_request(request_id)
    if row is None:
        h._json({
            "request_id": request_id,
            "mb_release_group_id": None,
            "status": RESOLVE_STATUS_NOT_FOUND,
            "error": f"request {request_id} not found",
        }, status=404)
        return

    existing_rg = row.get("mb_release_group_id")
    if existing_rg:
        h._json({
            "request_id": request_id,
            "mb_release_group_id": existing_rg,
            "status": RESOLVE_STATUS_RESOLVED,
        })
        return

    mb_release_id = row.get("mb_release_id")
    if not mb_release_id:
        h._json({
            "request_id": request_id,
            "mb_release_group_id": None,
            "status": RESOLVE_STATUS_MISSING_RELEASE_ID,
            "error": (
                f"request {request_id} has no mb_release_id to resolve"
            ),
        }, status=422)
        return

    # Mirror transient errors (network, JSON decode) are retryable, on
    # either mirror. See
    # ``lib/mbid_replace_service.py::_TRANSIENT_LOOKUP_EXCEPTIONS`` for
    # the rationale and the same exception set.
    import socket as _socket
    from urllib.error import URLError
    transient: tuple[type[BaseException], ...] = (
        URLError, TimeoutError, _socket.timeout, ConnectionError,
        json.JSONDecodeError,
    )

    # MB release ids are UUIDs; numeric ids are Discogs-pathway, whose
    # release-group analog is the Discogs master (KTD-1: the numeric
    # master id lives in this same column, per the
    # ``lib/field_resolver_service.py::_looks_numeric`` convention).
    release_source = detect_release_source(mb_release_id)
    if release_source == "unknown":
        h._json({
            "request_id": request_id,
            "mb_release_group_id": None,
            "status": RESOLVE_STATUS_NON_MB_RELEASE_ID,
            "error": (
                f"request {request_id}.mb_release_id "
                f"{mb_release_id!r} is neither a MusicBrainz UUID "
                "nor a numeric Discogs id"
            ),
        }, status=422)
        return

    if release_source == "discogs":
        discogs_id_num = int(normalize_release_id(mb_release_id))

        from web.discogs import DiscogsMirrorNotConfigured

        # Bypass the 24h meta cache — this write path can persist the
        # resolved master into the pipeline DB, so it must read live
        # upstream state rather than a possibly-stale cached master
        # (same rationale as the add flow's ``fresh=True`` calls).
        try:
            discogs_data = discogs_api.get_release(
                discogs_id_num, fresh=True,
            )
        except DiscogsMirrorNotConfigured as exc:
            h._json({
                "request_id": request_id,
                "mb_release_group_id": None,
                "status": RESOLVE_STATUS_MIRROR_UNCONFIGURED,
                "error": f"Discogs mirror not configured: {exc}",
            }, status=503)
            return
        except transient as exc:
            h._json({
                "request_id": request_id,
                "mb_release_group_id": None,
                "status": RESOLVE_STATUS_TRANSIENT,
                "error": f"Discogs lookup failed (transient): {exc}",
            }, status=503)
            return
        except Exception as exc:  # noqa: BLE001
            h._json({
                "request_id": request_id,
                "mb_release_group_id": None,
                "status": RESOLVE_STATUS_LOOKUP_FAILED,
                "error": (
                    f"Discogs lookup for {mb_release_id} failed: {exc}"
                ),
            }, status=422)
            return

        master_id = (
            discogs_data.get("release_group_id")
            if isinstance(discogs_data, dict) else None
        )
        if not master_id:
            h._json({
                "request_id": request_id,
                "mb_release_group_id": None,
                "status": RESOLVE_STATUS_MASTERLESS,
            })
            return

        db.update_request_fields(request_id, mb_release_group_id=master_id)
        h._json({
            "request_id": request_id,
            "mb_release_group_id": master_id,
            "status": RESOLVE_STATUS_RESOLVED,
        })
        return

    try:
        data = mb_api.get_release(mb_release_id, fresh=False)
    except transient as exc:
        h._json({
            "request_id": request_id,
            "mb_release_group_id": None,
            "status": RESOLVE_STATUS_TRANSIENT,
            "error": f"MB lookup failed (transient): {exc}",
        }, status=503)
        return
    except Exception as exc:  # noqa: BLE001
        h._json({
            "request_id": request_id,
            "mb_release_group_id": None,
            "status": RESOLVE_STATUS_LOOKUP_FAILED,
            "error": (
                f"MB lookup for {mb_release_id} failed: {exc}"
            ),
        }, status=422)
        return

    rg_id = (data or {}).get("release_group_id") if isinstance(data, dict) else None
    if not rg_id:
        h._json({
            "request_id": request_id,
            "mb_release_group_id": None,
            "status": RESOLVE_STATUS_NO_RELEASE_GROUP,
            "error": (
                f"MB release {mb_release_id} has no release_group_id"
            ),
        }, status=422)
        return

    db.update_request_fields(request_id, mb_release_group_id=rg_id)
    h._json({
        "request_id": request_id,
        "mb_release_group_id": rg_id,
        "status": RESOLVE_STATUS_RESOLVED,
    })


class PipelineReplaceRequest(BaseModel):
    target_mb_release_id: str = Field(min_length=1)


def post_pipeline_replace(h, body: dict, req_id_str: str) -> None:
    """``POST /api/pipeline/<id>/replace``.

    Supersede the source request with a new row at ``target_mb_release_id``.
    Counterpart of ``pipeline-cli replace``. Both surfaces wrap
    ``MbidReplaceService.replace_request_mbid`` — keep them in sync (see
    ``CLAUDE.md`` § "CLI ⇄ API surface symmetry").

    Body: ``{"target_mb_release_id": "<id>"}`` — an MB release UUID or a
    Discogs numeric release id; must share the source's pathway (MB or
    Discogs) and release group/master.

    Status-code mapping mirrors the CLI exit codes:
      * 200 — ``RESULT_REPLACED``
      * 400 — body validation failure (missing/empty target)
      * 404 — ``RESULT_NOT_FOUND``
      * 409 — ``RESULT_WRONG_STATE`` (including supersede race —
              ``descendant_request_id`` populated so the UI can
              deep-link the operator to the new request) or
              ``RESULT_TARGET_COLLISION_REQUEST``
      * 422 — ``RESULT_TARGET_INVALID``, ``RESULT_TARGET_RELEASE_GROUP_MISMATCH``,
              ``RESULT_TARGET_SAME_AS_CURRENT``
      * 503 — ``RESULT_TRANSIENT`` (mirror unreachable etc.) or
              ``RESULT_MIRROR_UNCONFIGURED`` (Discogs mirror not configured)
    """
    from lib.config import read_runtime_config
    from lib.mbid_replace_service import (
        MbidReplaceService,
        RESULT_MIRROR_UNCONFIGURED,
        RESULT_NOT_FOUND,
        RESULT_REPLACED,
        RESULT_TARGET_COLLISION_REQUEST,
        RESULT_TARGET_INVALID,
        RESULT_TARGET_RELEASE_GROUP_MISMATCH,
        RESULT_TARGET_SAME_AS_CURRENT,
        RESULT_TRANSIENT,
        RESULT_WRONG_STATE,
    )

    try:
        request_id = int(req_id_str)
    except (TypeError, ValueError):
        h._error("Invalid request id")
        return

    req_body = parse_body(h, body, PipelineReplaceRequest)
    if req_body is None:
        return
    target = req_body.target_mb_release_id.strip()
    if not target:
        h._json({
            "error": "target_mb_release_id must be a non-empty string",
        }, status=400)
        return

    db = _server()._db()
    cfg = read_runtime_config()
    svc = MbidReplaceService(db=db, config=cfg)
    result = svc.replace_request_mbid(
        request_id, target_mb_release_id=target,
    )

    payload: dict[str, object] = {
        "outcome": result.outcome,
        "request_id": result.request_id,
        "new_request_id": result.new_request_id,
        "current_status": result.current_status,
        "descendant_request_id": result.descendant_request_id,
        "error_message": result.error_message,
        "reason": result.reason,
        "warnings": list(result.warnings),
    }
    if result.outcome == RESULT_REPLACED:
        h._json(payload)
        return
    if result.outcome == RESULT_NOT_FOUND:
        payload["error"] = result.error_message or "Not found"
        h._json(payload, status=404)
        return
    if result.outcome in (
        RESULT_WRONG_STATE,
        RESULT_TARGET_COLLISION_REQUEST,
    ):
        payload["error"] = result.error_message or "Wrong state"
        h._json(payload, status=409)
        return
    if result.outcome in (
        RESULT_TARGET_INVALID,
        RESULT_TARGET_RELEASE_GROUP_MISMATCH,
        RESULT_TARGET_SAME_AS_CURRENT,
    ):
        payload["error"] = result.error_message or "Semantic violation"
        h._json(payload, status=422)
        return
    if result.outcome in (RESULT_TRANSIENT, RESULT_MIRROR_UNCONFIGURED):
        payload["error"] = result.error_message or "Service unavailable; retry"
        h._json(payload, status=503)
        return
    h._error(f"Unknown replace outcome: {result.outcome}", 500)


ROUTES: list[RouteRegistration] = [
    pattern_route(
        "POST", r"^/api/pipeline/(\d+)/replace$", post_pipeline_replace,
        "Supersede the source request with a new row at a different "
        "release id (MB UUID or Discogs numeric id) in the same "
        "release group/master, same pathway as the source.",
        classified=True,
    ),
    pattern_route(
        "POST", r"^/api/pipeline/(\d+)/resolve-rg$", post_pipeline_resolve_rg,
        "Lazy-backfill mb_release_group_id for a legacy request row.",
        classified=True,
    ),
]
