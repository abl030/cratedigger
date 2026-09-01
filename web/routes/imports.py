"""Operator import route handlers — force import and wrong matches."""

import os
from collections.abc import Callable
from email.message import Message
from io import BufferedIOBase
from typing import Any, Protocol, Self, TypedDict

from pydantic import BaseModel, ConfigDict, Field, model_validator

from lib import transitions
from lib.config import read_runtime_config
from lib.force_import_service import (
    RESULT_PROCESSING_LOCKED,
    RESULT_QUEUED,
    enqueue_force_import,
)
from lib.fs_authority import OpenedRegularFile
from lib.import_preview import (
    ImportPreviewValues,
    preview_import_from_download_log,
    preview_import_from_values,
)
from lib.util import observe_failed_path
from lib.validation_envelope import (
    ValidationResultEnvelope,
    decode_validation_envelope,
)
from lib.wrong_match_cleanup_service import (
    cleanup_all_wrong_matches,
)
from lib.wrong_match_delete_service import (
    OUTCOME_DELETE_FAILED as DELETE_OUTCOME_FAILED,
)
from lib.wrong_match_delete_service import (
    OUTCOME_SKIPPED_ACTIVE_JOB as DELETE_OUTCOME_ACTIVE_JOB,
)
from lib.wrong_match_delete_service import (
    OUTCOME_SKIPPED_INVALID_ROW as DELETE_OUTCOME_INVALID_ROW,
)
from lib.wrong_match_delete_service import (
    OUTCOME_SKIPPED_LOCKED as DELETE_OUTCOME_LOCKED,
)
from lib.wrong_match_delete_service import (
    OUTCOME_SKIPPED_NOT_VISIBLE as DELETE_OUTCOME_NOT_VISIBLE,
)
from lib.wrong_match_delete_service import (
    OUTCOME_SKIPPED_PATH_UNAVAILABLE as DELETE_OUTCOME_PATH_UNAVAILABLE,
)
from lib.wrong_match_delete_service import (
    OUTCOME_SKIPPED_UNSAFE_PATH as DELETE_OUTCOME_UNSAFE_PATH,
)
from lib.wrong_match_delete_service import (
    WrongMatchDeleteDB,
    WrongMatchDeleteResult,
    WrongMatchDeleteSummary,
    delete_wrong_match,
    delete_wrong_match_group,
)
from web.overlay import compute_library_rank
from web.routes._pydantic import parse_body
from web.routes._registry import RouteHandler, RouteRegistration, route
from web.routes.pipeline import _serialize_import_job
from web.runtime import runtime
from web.triage_runner import TriageRunner
from web.wrong_match_file_service import (
    WrongMatchSourceRefused,
    WrongMatchSourceUnavailable,
    build_wrong_match_explorer,
    resolve_wrong_match_stream_file,
)
from web.wrong_match_queue_view import build_wrong_match_groups


def _threshold_milli(value: object) -> int:
    try:
        parsed = int(value) if isinstance(value, (str, int, float)) else 180
    except (TypeError, ValueError):
        parsed = 180
    return max(0, min(parsed, 999))


def _is_green_distance(
    vr: ValidationResultEnvelope,
    threshold_milli: int,
) -> bool:
    return vr.distance is not None and vr.distance <= threshold_milli / 1000


def get_wrong_matches(h: RouteHandler, params: dict[str, list[str]]) -> None:
    """Return grouped wrong-match rejections for the manual-review UI.

    ``?include_replaced=true`` opts into showing rows whose parent
    ``album_requests`` row is ``status='replaced'``. The default
    (``false``) filters them out so the Wrong Matches tab focuses on
    actionable rejections, not frozen audit history (R31).
    """
    include_replaced = (
        params.get("include_replaced", ["false"])[0].lower() == "true"
    )
    rt = runtime()
    h._json({"groups": build_wrong_match_groups(
        db=rt.db(),
        check_beets_library_detail=rt.check_beets_library_detail,
        compute_library_rank=compute_library_rank,
        include_replaced=include_replaced,
    )})


def _download_log_id_from_params(params: dict[str, list[str]]) -> int:
    raw_id = params.get("download_log_id", [""])[0]
    try:
        return int(raw_id)
    except (TypeError, ValueError) as exc:
        raise ValueError("download_log_id must be an integer") from exc


def _byte_range(range_header: str | None, size: int) -> tuple[int, int, int] | None:
    if not range_header:
        return None
    if not range_header.startswith("bytes="):
        raise ValueError("Only bytes ranges are supported")

    raw = range_header[6:].strip()
    if "," in raw:
        raise ValueError("Multiple ranges are not supported")
    start_raw, sep, end_raw = raw.partition("-")
    if not sep:
        raise ValueError("Invalid range")

    if not start_raw:
        suffix = int(end_raw)
        if suffix <= 0:
            raise ValueError("Invalid suffix range")
        start = max(size - suffix, 0)
        end = size - 1
    else:
        start = int(start_raw)
        end = size - 1 if not end_raw else int(end_raw)

    if start < 0 or end < start or start >= size:
        raise ValueError("Range out of bounds")
    end = min(end, size - 1)
    return start, end, (end - start) + 1


def get_wrong_match_explorer(h: RouteHandler, params: dict[str, list[str]]) -> None:
    """Return filesystem-backed file/tag explorer data for one wrong match."""
    try:
        log_id = _download_log_id_from_params(params)
    except ValueError as exc:
        h._error(str(exc))
        return

    entry = runtime().db().get_download_log_entry(log_id)
    if not entry:
        h._error(f"Download log entry {log_id} not found", 404)
        return

    try:
        payload = build_wrong_match_explorer(
            download_log_id=log_id,
            entry=entry,
        )
    except WrongMatchSourceUnavailable as exc:
        # Refused observation: retryable world failure, never "not found".
        h._error(str(exc), 503)
        return
    except WrongMatchSourceRefused as exc:
        # Containment decision (issue #1099): the name may exist, we
        # refuse to read it. Neither a definitive absence (404) nor a
        # retryable world failure (503) — a semantic violation (422).
        h._error(str(exc), 422)
        return
    except FileNotFoundError as exc:
        h._error(str(exc), 404)
        return
    h._json(payload)


class _StreamingRouteHandler(RouteHandler, Protocol):
    """``RouteHandler`` plus the raw ``BaseHTTPRequestHandler`` surface
    ``get_wrong_match_audio`` needs for manual byte-range streaming
    (headers/response-line/body writes bypass the ``_json``/``_error``
    envelope entirely). The real ``web/server.py::Handler`` satisfies
    this structurally, same as ``RouteHandler`` itself.
    """

    headers: Message
    wfile: BufferedIOBase
    close_connection: bool

    def send_response(self, code: int, message: str | None = None) -> None: ...

    def send_header(self, keyword: str, value: str) -> None: ...

    def end_headers(self) -> None: ...


def get_wrong_match_audio(
    h: _StreamingRouteHandler,
    params: dict[str, list[str]],
    *,
    stream_file_resolver: (
        Callable[..., tuple[OpenedRegularFile, str]] | None
    ) = None,
) -> None:
    """Stream one wrong-match audio file with byte-range support."""
    try:
        log_id = _download_log_id_from_params(params)
    except ValueError as exc:
        h._error(str(exc))
        return

    relative_path = params.get("path", [""])[0]
    if not relative_path:
        h._error("Missing path")
        return

    entry = runtime().db().get_download_log_entry(log_id)
    if not entry:
        h._error(f"Download log entry {log_id} not found", 404)
        return

    try:
        resolve_stream = stream_file_resolver or resolve_wrong_match_stream_file
        opened, mime_type = resolve_stream(
            entry=entry,
            relative_path=relative_path,
        )
    except ValueError as exc:
        h._error(str(exc))
        return
    except WrongMatchSourceUnavailable as exc:
        h._error(str(exc), 503)
        return
    except WrongMatchSourceRefused as exc:
        # Containment decision (issue #1099): same 422 verdict as the
        # explorer, for the identical reason.
        h._error(str(exc), 422)
        return
    except FileNotFoundError as exc:
        h._error(str(exc), 404)
        return

    try:
        size = opened.stat_result.st_size
        try:
            requested_range = _byte_range(h.headers.get("Range"), size)
        except ValueError:
            h.send_response(416)
            h.send_header("Content-Range", f"bytes */{size}")
            h.send_header("Content-Length", "0")
            h.end_headers()
            return

        start = 0
        end = size - 1
        content_length = size
        status = 200
        if requested_range is not None:
            start, end, content_length = requested_range
            status = 206

        h.send_response(status)
        h.send_header("Content-Type", mime_type)
        h.send_header("Content-Length", str(content_length))
        h.send_header("Accept-Ranges", "bytes")
        h.send_header("Cache-Control", "no-cache")
        if requested_range is not None:
            h.send_header("Content-Range", f"bytes {start}-{end}/{size}")
        h.end_headers()

        os.lseek(opened.fd, start, os.SEEK_SET)
        remaining = content_length
        while remaining > 0:
            chunk = os.read(opened.fd, min(64 * 1024, remaining))
            if not chunk:
                break
            h.wfile.write(chunk)
            remaining -= len(chunk)
        if remaining > 0:
            # Short read (file truncated mid-stream): the body is shorter
            # than the declared Content-Length, so the keep-alive stream is
            # desynced — never reuse this socket.
            h.close_connection = True
    finally:
        opened.close()


def _delete_wrong_match_row(
    pdb: WrongMatchDeleteDB, log_id: int,
) -> WrongMatchDeleteResult:
    """Converge helper: operator-authority delete via lib/wrong_match_delete_service.

    Do NOT route this through cleanup_wrong_match. Converge has already collected
    operator intent (they picked the green candidate; everything else dies). The
    evidence-based cleanup classifier would skip kept_would_import / stale-evidence
    rows that converge is explicitly trying to clear. See post_wrong_match_converge
    docstring.
    """
    return delete_wrong_match(pdb, log_id, require_visible=True)


class WrongMatchDeleteRequest(BaseModel):
    download_log_id: int = Field(gt=0)


def post_wrong_match_delete(h: RouteHandler, body: dict[str, object]) -> None:
    """Operator-triggered deletion of one visible Wrong Matches candidate."""
    req_body = parse_body(h, body, WrongMatchDeleteRequest)
    if req_body is None:
        return
    log_id = req_body.download_log_id

    result = delete_wrong_match(runtime().db(), log_id, require_visible=True)
    if result.success:
        h._json({"status": "ok", **result.to_dict()})
        return
    # Every refusal keeps the whole typed result. ``{"error": ...}`` alone
    # cannot express "operational refusal has neither deleted_path nor
    # path_missing and is non-success" — and the #1063 reproduction was
    # captured with ``--json`` (issue #1063 review T3.1).
    _wrong_match_delete_error(h, result)


def _wrong_match_delete_error(
    h: RouteHandler, result: WrongMatchDeleteResult,
) -> None:
    status = _WRONG_MATCH_DELETE_ERROR_STATUS.get(result.outcome, 500)
    error = (
        "active_import_job"
        if result.outcome == DELETE_OUTCOME_ACTIVE_JOB
        else result.error or result.reason or result.outcome
    )
    # Spread FIRST: the typed result carries its own ``error`` key (often
    # ``None``), which would otherwise overwrite the message.
    h._json(
        {**result.to_dict(), "status": "error", "error": error},
        status=status,
    )


_WRONG_MATCH_DELETE_ERROR_STATUS: dict[str, int] = {
    DELETE_OUTCOME_ACTIVE_JOB: 409,
    # Retryable world failures, not verdicts about the folder.
    DELETE_OUTCOME_PATH_UNAVAILABLE: 503,
    DELETE_OUTCOME_LOCKED: 503,
    DELETE_OUTCOME_INVALID_ROW: 404,
    DELETE_OUTCOME_NOT_VISIBLE: 404,
    DELETE_OUTCOME_UNSAFE_PATH: 422,
}


class WrongMatchDeleteGroupRequest(BaseModel):
    request_id: int = Field(gt=0)


def post_wrong_match_delete_group(
    h: RouteHandler, body: dict[str, object],
) -> None:
    """Operator-triggered deletion of all current Wrong Matches for a request."""
    req_body = parse_body(h, body, WrongMatchDeleteGroupRequest)
    if req_body is None:
        return
    request_id = req_body.request_id

    summary = delete_wrong_match_group(runtime().db(), request_id)
    status = "ok" if summary.success else "partial"
    h._json(
        {"status": status, **summary.to_dict()},
        status=_wrong_match_delete_group_http_status(summary),
    )


def _wrong_match_delete_group_http_status(summary: WrongMatchDeleteSummary) -> int:
    """Precedence the routed CLI adapter maps straight back to exit codes.

    This route IS the group-delete execution path for both surfaces
    (issue #1063), so the mapping below plus
    ``_WRONG_MATCH_DELETE_EXIT_OVERRIDES`` in
    ``scripts/pipeline_cli/wrong_match.py`` is the whole CLI ⇄ API
    exit-code contract.
    """
    if summary.success:
        return 200
    outcomes = {result.outcome for result in summary.results}
    if DELETE_OUTCOME_FAILED in outcomes:
        return 500
    if DELETE_OUTCOME_PATH_UNAVAILABLE in outcomes:
        return 503
    if DELETE_OUTCOME_LOCKED in outcomes:
        return 503
    if DELETE_OUTCOME_ACTIVE_JOB in outcomes:
        return 409
    if DELETE_OUTCOME_UNSAFE_PATH in outcomes:
        return 422
    if outcomes & {DELETE_OUTCOME_INVALID_ROW, DELETE_OUTCOME_NOT_VISIBLE}:
        return 404
    # Not success and no recognized outcome: the summary contradicts
    # itself, which is a server fault (and the CLI's own residual exit
    # code for the same shape is 1, which 500 maps back to).
    return 500


class _GreenCandidate(TypedDict):
    """One green-distance force-import candidate assembled below."""

    download_log_id: int
    distance: float | None


class WrongMatchConvergeRequest(BaseModel):
    request_id: int = Field(gt=0)
    threshold_milli: Any = None


def post_wrong_match_converge(h: RouteHandler, body: dict[str, object]) -> None:
    """Queue acceptable candidates and delete the rest for the release.

    ⚠ OPERATOR-AUTHORITY CONTRACT — do not route deletion through
    cleanup_wrong_match or the evidence-based cleanup classifier.

    Converge is a one-click cleanup workflow: the operator has reviewed the
    candidates, chosen the green (acceptable-distance) ones for force-import,
    and is explicitly asking us to remove the rest. Their judgement, not the
    classifier's, gates the deletion. The unmatched rows are deleted via
    lib/wrong_match_delete_service.delete_wrong_match, which preserves
    advisory-lock + active-jobs safety but skips the candidate-evidence load,
    the reducer, and the verified-lossless short-circuit.

    Regression history: routing converge through cleanup_wrong_match caused
    "kept_would_import" and stale-evidence rows to silently stay visible after
    the operator hit converge — visible as a #268 follow-up bug. The fix is
    permanent; if you find yourself reaching for cleanup_wrong_match here,
    re-read this docstring.
    """
    req_body = parse_body(h, body, WrongMatchConvergeRequest)
    if req_body is None:
        return
    rid = req_body.request_id

    threshold_milli = _threshold_milli(req_body.threshold_milli)
    # Converge is intentionally a one-click cleanup workflow: green rows are
    # queued, and non-green rows for the same release are removed immediately.
    # Keep accepting the legacy field from older clients, but do not let it
    # leave high-distance leftovers behind.
    delete_unmatched = True

    rt = runtime()
    pdb = rt.db()
    req = pdb.get_request(rid)
    if not req:
        h._error(f"Request {rid} not found", 404)
        return

    selected: list[dict[str, object]] = []
    unmatched: list[dict[str, object]] = []
    green_candidates: list[_GreenCandidate] = []
    skipped: list[dict[str, object]] = []
    jobs: list[dict[str, object]] = []
    unmatched_log_ids: list[int] = []
    deduped = 0
    dismissed = 0
    deleted = 0
    remaining = 0

    for row in pdb.get_wrong_matches():
        if row.get("request_id") != rid:
            continue
        # ``download_log_id`` is a required, non-nullable ``download_log.id``
        # column (WrongMatchCandidateRow), so the row type already proves
        # this is an ``int``.
        lid = row["download_log_id"]

        vr = decode_validation_envelope(row.get("validation_result"))
        distance = vr.distance
        observation = observe_failed_path(vr.failed_path or "")
        if observation.indeterminate:
            # Neither green nor unmatched: we cannot see this source, so
            # we neither import it nor delete it, and the green count the
            # operator reads must agree with that (issue #1063 T4.5 —
            # ``web/js/wrong-matches.js::isConvergeGreen`` excludes the
            # same entries client-side).
            skipped.append({
                "download_log_id": lid,
                "reason": "path_unavailable",
                "detail": observation.unavailable_reason(),
            })
            remaining += 1
            continue
        green = _is_green_distance(vr, threshold_milli)

        if green:
            green_candidates.append({
                "download_log_id": lid,
                "distance": distance,
            })
            continue

        unmatched.append({
            "download_log_id": lid,
            "distance": distance,
        })
        unmatched_log_ids.append(lid)

    for candidate in green_candidates:
        lid = candidate["download_log_id"]
        result = enqueue_force_import(pdb, read_runtime_config(), lid)
        if result.outcome != RESULT_QUEUED:
            skipped_entry: dict[str, object] = {
                "download_log_id": lid,
                "reason": result.outcome,
            }
            if result.outcome == RESULT_PROCESSING_LOCKED:
                owner = transitions.processing_owner_payload(
                    result.processing_owner
                )
                if owner is None:
                    raise RuntimeError(
                        "processing-locked force import is missing its "
                        "exact owner"
                    )
                skipped_entry["processing_owner"] = owner
            skipped.append(skipped_entry)
            remaining += 1
            continue
        assert result.job is not None
        job = result.job
        if getattr(job, "deduped", False):
            deduped += 1
        jobs.append(_serialize_import_job(job))
        selected.append({
            "download_log_id": lid,
            "distance": candidate["distance"],
            "job_id": job.id,
            "deduped": bool(getattr(job, "deduped", False)),
        })
        remaining += 1

    if selected:
        for lid in unmatched_log_ids:
            result = _delete_wrong_match_row(pdb, lid)
            if result.success:
                deleted += 1
            else:
                skipped.append({
                    "download_log_id": lid,
                    "reason": result.outcome,
                    "delete_reason": result.reason,
                    "delete_error": result.error,
                })
                remaining += 1
    else:
        for _lid in unmatched_log_ids:
            remaining += 1

    h._json({
        "status": "ok",
        "request_id": rid,
        "threshold_milli": threshold_milli,
        "threshold": threshold_milli / 1000,
        "delete_unmatched": delete_unmatched,
        "selected_count": len(selected),
        "unmatched_count": len(unmatched),
        "queued": len(jobs),
        "deduped": deduped,
        "dismissed": dismissed,
        "deleted": deleted,
        "remaining": remaining,
        "group_empty": remaining == 0,
        "selected": selected,
        "unmatched": unmatched,
        "skipped": skipped,
        "jobs": jobs,
    }, status=202)


class ImportPreviewValuesRequest(BaseModel):
    """Strict HTTP representation of ``ImportPreviewValues``.

    The CLI intentionally retains its explicit filesystem path mode.  HTTP
    gets only simulation values or a server-owned download-log identifier.
    """

    model_config = ConfigDict(strict=True, extra="forbid")

    is_flac: bool = False
    min_bitrate: int | None = None
    is_cbr: bool = False
    is_vbr: bool | None = None
    avg_bitrate: int | None = None
    spectral_grade: str | None = None
    spectral_bitrate: int | None = None
    existing_min_bitrate: int | None = None
    existing_avg_bitrate: int | None = None
    existing_spectral_bitrate: int | None = None
    existing_spectral_grade: str | None = None
    override_min_bitrate: int | None = None
    existing_format: str | None = None
    existing_is_cbr: bool = False
    post_conversion_min_bitrate: int | None = None
    post_conversion_is_cbr: bool | None = None
    converted_count: int = 0
    candidate_verified_lossless_proof: bool = False
    verified_lossless_target: str | None = None
    target_format: str | None = None
    new_format: str | None = None
    audio_check_mode: str = "normal"
    audio_corrupt: bool = False
    has_nested_audio: bool = False
    candidate_v0_probe_avg: int | None = None
    candidate_v0_probe_min: int | None = None
    existing_v0_probe_avg: int | None = None
    candidate_v0_probe_kind: str | None = None
    existing_v0_probe_kind: str | None = None
    supported_lossless_source: bool | None = None


class ImportPreviewRequest(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    values: ImportPreviewValuesRequest | None = None
    download_log_id: int | None = Field(default=None, gt=0)

    @model_validator(mode="after")
    def _one_mode(self) -> Self:
        if (self.values is None) == (self.download_log_id is None):
            raise ValueError("provide exactly one of values or download_log_id")
        return self


def post_import_preview(h: RouteHandler, body: dict[str, object]) -> None:
    """HTTP preview: strict nested values or a server-owned log id only."""
    request = parse_body(h, body, ImportPreviewRequest)
    if request is None:
        return
    if request.values is not None:
        from lib.config import read_runtime_rank_config
        preview = preview_import_from_values(
            ImportPreviewValues(**request.values.model_dump()),
            cfg=read_runtime_rank_config(),
        )
    else:
        assert request.download_log_id is not None
        preview = preview_import_from_download_log(
            runtime().db(), request.download_log_id,
        )
    h._json(preview.to_dict())


class WrongMatchTriageRequest(BaseModel):
    """Confirmation guard so the destructive triage isn't accidental.

    ``confirm_all_wrong_matches`` must be the literal ``True``; Pydantic
    enforces type-and-value via a model validator (a plain ``bool``
    default would let ``False`` through).
    """

    confirm_all_wrong_matches: bool

    @model_validator(mode="after")
    def _must_be_true(self) -> Self:
        if self.confirm_all_wrong_matches is not True:
            raise ValueError("confirm_all_wrong_matches must be true")
        return self


# Module singleton: at most one bulk sweep at a time, status shared
# between the POST trigger and the GET status poller. In-memory only —
# a web restart aborts the sweep, same as the old synchronous handler.
_triage_runner = TriageRunner()


def post_wrong_match_triage(h: RouteHandler, body: dict[str, object]) -> None:
    """Start the bulk triage sweep on a background thread (202).

    The sweep takes minutes when stale rows trigger re-measurement
    (#271); running it inline wedged the single-threaded server for the
    duration. The sweep thread gets its handle by entering
    ``runtime().open_background_db`` — under a DSN that is a connection
    of its own, because one handle is one PostgreSQL session and sharing
    the request thread's would put both threads inside the same
    session-level advisory locks. A DSN-less runtime (the dev server,
    the test harness) has only the injected handle and does share it.
    Either way the runtime, not the runner, owns that handle's lifetime.
    """
    req_body = parse_body(h, body, WrongMatchTriageRequest)
    if req_body is None:
        return
    started = _triage_runner.start(
        db_session=runtime().open_background_db,
        cleanup_fn=cleanup_all_wrong_matches,
    )
    if not started:
        h._error("triage sweep already running", status=409)
        return
    h._json({"status": "started", "state": "running"}, status=202)


def get_wrong_match_triage_status(
    h: RouteHandler, params: dict[str, list[str]],
) -> None:
    h._json(_triage_runner.status())


class WrongMatchTriageCancelRequest(BaseModel):
    """Whether this cancel may ARM the sticky pending-cancel slot when
    nothing is currently running (issue #1106 F3).

    Only the CLI's own ``Ctrl-C`` handler
    (``scripts/pipeline_cli/wrong_match.py::cmd_wrong_match_triage``) sets
    this true — it is specifically racing its OWN still-in-flight start
    POST. Every other caller (the web UI's Stop button, the standalone
    ``wrong-match-triage-cancel`` command) defaults false: an unarmed
    cancel with nothing running is a pure #1083 no-op and must never
    affect a sweep it did not itself observe running.
    """

    model_config = ConfigDict(strict=True, extra="forbid")

    arm_pending: bool = False


def post_wrong_match_triage_cancel(
    h: RouteHandler, body: dict[str, object],
) -> None:
    """Request cancellation of the in-flight bulk triage sweep (issue #1083).

    No confirmation is needed — cancellation only ever stops destructive
    work in progress, it never starts any. Always 200: a cancel with no
    sweep running, and one racing a sweep that is already recording its
    own terminal state, both just return the current ``status()``
    snapshot rather than a 409 (there is nothing "wrong" in either
    case). The CLI's ``Ctrl-C`` handler and the web UI's Stop button are
    the two callers, over the exact same route; only the former sends
    ``arm_pending: true`` (issue #1106 F3).
    """
    req_body = parse_body(h, body, WrongMatchTriageCancelRequest)
    if req_body is None:
        return
    h._json(_triage_runner.cancel(arm_pending=req_body.arm_pending))


ROUTES: list[RouteRegistration] = [
    route(
        "GET", "/api/wrong-matches", get_wrong_matches,
        "Wrong-match queue — rejected downloads grouped by request, with "
        "per-entry quality + on-disk fields for operator review.",
        classified=True,
    ),
    route(
        "GET", "/api/wrong-matches/audio", get_wrong_match_audio,
        "Stream one wrong-match audio file with byte-range support.",
        classified=True,
    ),
    route(
        "GET", "/api/wrong-matches/triage/status",
        get_wrong_match_triage_status,
        "Status of the background bulk-triage sweep — state plus the "
        "cleanup summary once completed.",
        classified=True,
    ),
    route(
        "GET", "/api/wrong-matches/explorer", get_wrong_match_explorer,
        "Filesystem-backed file/tag explorer payload for one wrong match.",
        classified=True,
    ),
    route(
        "POST", "/api/import-preview", post_import_preview,
        "Preview whether an import would pass — accepts exactly one strict "
        "typed values object or a positive server-owned download_log_id.",
        classified=True,
    ),
    route(
        "POST", "/api/wrong-matches/delete", post_wrong_match_delete,
        "Operator-triggered deletion of one visible Wrong Matches "
        "candidate (DESTRUCTIVE on disk).",
        classified=True,
    ),
    route(
        "POST", "/api/wrong-matches/delete-group",
        post_wrong_match_delete_group,
        "Operator-triggered deletion of all current Wrong Matches for "
        "a request (DESTRUCTIVE on disk).",
        classified=True,
    ),
    route(
        "POST", "/api/wrong-matches/converge", post_wrong_match_converge,
        "Queue acceptable candidates for force-import and delete the "
        "rest for one request (one-click cleanup).",
        classified=True,
    ),
    route(
        "POST", "/api/wrong-matches/triage", post_wrong_match_triage,
        "Start the full Wrong Matches cleanup sweep on a background "
        "thread (DESTRUCTIVE); requires confirm_all_wrong_matches=true. "
        "Returns 202 immediately; poll /api/wrong-matches/triage/status.",
        classified=True,
    ),
    route(
        "POST", "/api/wrong-matches/triage/cancel",
        post_wrong_match_triage_cancel,
        "Request cancellation of the in-flight bulk triage sweep, if "
        "any (issue #1083). Always 200 — a cancel with nothing running "
        "or one racing a sweep's own completion both just return the "
        "current status snapshot, never a 409. Optional "
        "{\"arm_pending\": true} (issue #1106) arms a sticky pending "
        "cancel that pre-cancels the very next sweep start admitted "
        "within a short window — reserved for the CLI's own Ctrl-C "
        "handler; omitted/false is a pure no-op when nothing is running.",
        classified=True,
    ),
]
