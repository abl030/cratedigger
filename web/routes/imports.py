"""Operator import route handlers — force import and wrong matches."""

import json
import os
from collections.abc import Mapping, Sequence
from email.message import Message
from io import BufferedIOBase
from typing import Any, Protocol, TypedDict

import msgspec
from pydantic import BaseModel, Field, model_validator

from web.routes._pydantic import parse_body

from lib.quality import _is_explicit_label
from lib.import_queue import (
    IMPORT_JOB_FORCE,
    ImportJob,
    force_import_dedupe_key,
    force_import_payload,
)
from lib.util import resolve_failed_path
from lib.wrong_match_cleanup_service import (
    cleanup_all_wrong_matches,
)
from lib.wrong_matches import wrong_match_row_is_visible
from lib.wrong_match_delete_service import (
    OUTCOME_DELETE_FAILED as DELETE_OUTCOME_FAILED,
    OUTCOME_SKIPPED_ACTIVE_JOB as DELETE_OUTCOME_ACTIVE_JOB,
    OUTCOME_SKIPPED_INVALID_ROW as DELETE_OUTCOME_INVALID_ROW,
    OUTCOME_SKIPPED_LOCKED as DELETE_OUTCOME_LOCKED,
    OUTCOME_SKIPPED_NOT_VISIBLE as DELETE_OUTCOME_NOT_VISIBLE,
    OUTCOME_SKIPPED_UNSAFE_PATH as DELETE_OUTCOME_UNSAFE_PATH,
    WrongMatchDeleteDB,
    WrongMatchDeleteResult,
    WrongMatchDeleteSummary,
    delete_wrong_match,
    delete_wrong_match_group,
)
from lib.import_preview import (
    ImportPreviewValues,
    preview_import_from_download_log,
    preview_import_from_path,
    preview_import_from_values,
)
from lib.validation_envelope import (
    ValidationResultEnvelope,
    decode_validation_envelope,
)
from web.routes.pipeline import _serialize_import_job
from web.routes._registry import RouteHandler, RouteRegistration, route
from web.routes._server_access import _server
from web.triage_runner import TriageRunner
from web.wrong_match_file_service import (
    build_wrong_match_explorer,
    resolve_wrong_match_stream_file,
    source_dirs_from_validation_result,
    target_candidate,
)


def _row_presence(
    row: Mapping[str, object],
    beets_info: dict[str, dict[str, object]],
) -> str:
    """Answer 'is this release on disk?' for a wrong-matches row.

    Returns ``'exact'`` if the pipeline row's ``mb_release_id`` appears
    in the batched exact-hit lookup (via ``check_beets_library_detail``
    → ``BeetsDB.check_mbids_detail``), otherwise ``'absent'``. Matches
    the vocabulary of ``BeetsDB.ReleaseLocation.kind`` (issues #121 /
    #123).

    Issue #123 deleted the fuzzy artist+album fallback that used to
    return ``'fuzzy'``. It conflated identity with presence and
    silently attributed stale quality fields from sibling pressings
    to the badge. 'In library' now means exact-ID match, period.
    """
    mbid = row.get("mb_release_id")
    if isinstance(mbid, str) and mbid and mbid in beets_info:
        return "exact"
    return "absent"


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


# Numeric rank for the per-entry sort. Higher = better quality. Mirrors
# QualityRank's integer ordering but keeps the route layer free of an
# enum import; quality_rank strings here come from
# web.server.compute_library_rank which is the single producer.
_RANK_SORT_ORDER: dict[str, int] = {
    "lossless":    7,
    "transparent": 6,
    "excellent":   5,
    "good":        4,
    "acceptable":  3,
    "poor":        2,
    "unknown":     1,
}


def _entry_sort_key(entry: dict[str, object]) -> tuple[int, float, int]:
    """Best-quality first; ties broken by distance asc, id desc."""
    rank_name = entry.get("quality_rank")
    rank_value = _RANK_SORT_ORDER.get(rank_name, 0) \
        if isinstance(rank_name, str) else 0
    distance = entry.get("distance")
    distance_sort = float(distance) \
        if isinstance(distance, (int, float)) and not isinstance(distance, bool) \
        else float("inf")
    log_id = entry.get("download_log_id")
    log_id_int = log_id if isinstance(log_id, int) else 0
    return (-rank_value, distance_sort, -log_id_int)


def _quality_summary(row: Mapping[str, object],
                     beets_info: dict[str, dict[str, object]],
                     presence: str,
                     ) -> dict[str, object]:
    """Describe the album's current on-disk quality for a group header.

    Beets is the source of truth for format and bitrate when the album is
    imported; the pipeline DB carries the spectral + verified-lossless signal
    (those never live in beets). We combine them so the user can see at a
    glance whether force-importing is worthwhile.

    On-disk quality is reported only when ``presence == "exact"`` (issues
    #121 / #123). The fuzzy artist+album fallback was deleted — 'in
    library' now means exact-ID match, so ``presence != "exact"`` and
    ``"absent"`` are synonymous here (kept as a string to preserve the
    ``ReleaseLocation.kind`` vocabulary for the read side).
    """
    status = str(row.get("request_status") or "wanted")
    if presence != "exact":
        return {
            "status": status,
            "min_bitrate": None,
            "avg_bitrate": None,
            "format": None,
            "verified_lossless": False,
            "current_spectral_grade": None,
            "current_spectral_bitrate": None,
            "quality_label": None,
            "quality_rank": None,
        }

    srv = _server()
    mbid = row.get("mb_release_id")
    detail = beets_info.get(mbid) if isinstance(mbid, str) and mbid else None

    # Preserve the pipeline/beets minimum as explicit floor data. Current
    # labels and ranks use beets's positive-track average once imported.
    def _as_int(val: object) -> int | None:
        return val if isinstance(val, int) and not isinstance(val, bool) else None

    def _as_str(val: object) -> str | None:
        return val if isinstance(val, str) else None

    db_kbps = _as_int(row.get("request_min_bitrate"))
    beets_min_kbps = _as_int(detail.get("beets_bitrate")) if detail else None
    beets_avg_kbps = _as_int(detail.get("beets_avg_bitrate")) if detail else None
    fmt = _as_str(detail.get("beets_format")) if detail else None

    label: str | None = None
    rank: str | None = None
    if fmt:
        # Label is only meaningful with a bitrate; rank is meaningful from
        # format alone (falls through to the bare-codec band table).
        if beets_avg_kbps:
            from web.classify import average_quality_label
            label = average_quality_label(fmt, beets_avg_kbps)
        rank = srv.compute_library_rank(fmt, beets_avg_kbps)

    return {
        "status": status,
        "min_bitrate": beets_min_kbps if beets_min_kbps is not None else db_kbps,
        "avg_bitrate": beets_avg_kbps,
        "format": fmt,
        "verified_lossless": bool(row.get("request_verified_lossless") or False),
        "current_spectral_grade": row.get("request_current_spectral_grade"),
        "current_spectral_bitrate": row.get("request_current_spectral_bitrate"),
        "quality_label": label,
        "quality_rank": rank,
    }


_IMPORT_SUCCESS_OUTCOMES = ("success", "force_import", "manual_import")


def _latest_import_summary(rows: Sequence[Mapping[str, object]]
                           ) -> dict[str, object] | None:
    """Summary of the last successful import for a request.

    The expanded-group header describes what's currently on disk, not the most
    recent attempt. A rejection that happened after a successful import
    doesn't change what beets has — the earlier success is still the
    authoritative picture. Scan the newest-first history for the first
    active success/force_import row or historical manual_import row and
    surface its metadata.

    Returns ``None`` when the release has never been successfully imported.
    """
    if not rows:
        return None
    from datetime import datetime
    picked: Mapping[str, object] | None = None
    for row in rows:
        outcome = row.get("outcome")
        if isinstance(outcome, str) and outcome in _IMPORT_SUCCESS_OUTCOMES:
            picked = row
            break
    if picked is None:
        return None
    created_raw = picked.get("created_at")
    created: str | None = None
    if isinstance(created_raw, datetime):
        created = created_raw.isoformat()
    elif isinstance(created_raw, str):
        created = created_raw
    return {
        "id": picked.get("id"),
        "outcome": picked.get("outcome"),
        "created_at": created,
        "soulseek_username": picked.get("soulseek_username"),
        "actual_filetype": picked.get("actual_filetype"),
        "actual_min_bitrate": picked.get("actual_min_bitrate"),
        "beets_scenario": picked.get("beets_scenario"),
    }


def _build_wrong_match_groups(
    *, include_replaced: bool = False,
) -> list[dict[str, object]]:
    """Group wrong-match rejections by release (issue #113).

    Each ``album_requests`` row becomes one group; every rejected
    ``download_log`` entry with an on-disk ``failed_path`` becomes one entry
    inside its group. Groups with zero surviving entries are dropped so the
    UI only shows actionable work.

    Each group also carries an on-disk quality snapshot (format, bitrate,
    verified_lossless, spectral grade, rank tier) and the most-recent
    ``download_log`` row for the request, so the user can judge at a glance
    whether it's worth trying to force-import a rejected candidate.
    """
    srv = _server()
    pdb = srv._db()
    rows = pdb.get_wrong_matches()
    active_import_jobs = pdb.list_active_import_jobs(limit=200)
    active_jobs_by_log_id: dict[int, ImportJob] = {}
    active_jobs_by_request_id: dict[int, list[ImportJob]] = {}
    for job in active_import_jobs:
        payload = job.payload or {}
        request_id = job.request_id
        if isinstance(request_id, int):
            active_jobs_by_request_id.setdefault(request_id, []).append(job)
        download_log_id = payload.get("download_log_id")
        if isinstance(download_log_id, int):
            active_jobs_by_log_id[download_log_id] = job
    mbids = [
        mbid for row in rows
        for mbid in [row.get("mb_release_id")]
        if isinstance(mbid, str) and mbid
    ]
    beets_info = srv.check_beets_library_detail(mbids) if mbids else {}

    groups: dict[int, dict[str, object]] = {}
    group_entries: dict[int, list[dict[str, object]]] = {}
    order: list[int] = []

    for row in rows:
        if not wrong_match_row_is_visible(
            row,
            include_replaced=include_replaced,
        ):
            continue
        vr = decode_validation_envelope(row.get("validation_result"))
        failed_path = vr.failed_path or ""
        resolved_path = resolve_failed_path(failed_path)
        files_exist = resolved_path is not None
        if not files_exist:
            continue

        request_id = row["request_id"]
        assert isinstance(request_id, int)
        group = groups.get(request_id)
        if group is None:
            # Single seam for 'is this release on disk?' (issues #121 /
            # #123). ``_row_presence`` now returns just ``"exact"`` or
            # ``"absent"`` — the badge and the quality strip both gate
            # on exact-ID match, with no fuzzy escape hatch. Untagged
            # legacy copies honestly read 'not in library' now.
            presence = _row_presence(row, beets_info)
            in_library = presence == "exact"
            new_entries_list: list[dict[str, object]] = []
            group = {
                "request_id": request_id,
                "artist": row["artist_name"],
                "album": row["album_title"],
                "mb_release_id": row.get("mb_release_id"),
                "mb_release_group_id": row.get("mb_release_group_id"),
                "in_library": in_library,
                "pending_count": 0,
                "entries": new_entries_list,
                "import_jobs": [
                    _serialize_import_job(job)
                    for job in active_jobs_by_request_id.get(request_id, [])
                ],
                "latest_import": None,  # filled in after the loop
                **_quality_summary(row, beets_info, presence),
            }
            groups[request_id] = group
            group_entries[request_id] = new_entries_list
            order.append(request_id)

        target = target_candidate(vr)
        entries_list = group_entries[request_id]
        # ``download_log_id`` is a required, non-nullable ``download_log.id``
        # column (WrongMatchCandidateRow), so the row type already proves
        # this is an ``int``.
        log_id = row["download_log_id"]
        import_job = (
            _serialize_import_job(active_jobs_by_log_id[log_id])
            if log_id in active_jobs_by_log_id
            else None
        )
        # Per-candidate quality measurement comes from
        # album_quality_evidence via download_log.candidate_evidence_id
        # (joined in by PipelineDB.get_wrong_matches). Spectral grade /
        # V0 lineage are COALESCEd against the legacy denorm columns so
        # pre-evidence rows still surface what little they have.
        evidence_format = row.get("evidence_storage_format")
        evidence_min_bitrate = row.get("evidence_min_bitrate")
        evidence_avg_bitrate = row.get("evidence_avg_bitrate")
        configured_target = row.get("evidence_target_format")
        evidence_lineage_version = row.get("evidence_lineage_version")
        # New evidence stores downloaded-source format in storage_format and
        # target policy in target_format. Only explicitly marked historical
        # rows may use the old storage-label projection.
        evidence_contract = (
            configured_target
            if isinstance(configured_target, str) and configured_target
            else evidence_format
            if evidence_lineage_version == 1
            and isinstance(evidence_format, str)
            and _is_explicit_label(evidence_format)
            else None
        )
        # Current candidate ranking uses the evidence mean; min remains an
        # explicit floor in the payload for review/audit.
        entry_quality_rank = srv.compute_library_rank(
            evidence_format if isinstance(evidence_format, str) else None,
            evidence_avg_bitrate if isinstance(evidence_avg_bitrate, int) else None,
        )
        entries_list.append({
            "download_log_id": log_id,
            "failed_path": resolved_path or failed_path,
            "files_exist": files_exist,
            "distance": vr.distance,
            "scenario": vr.scenario,
            "detail": vr.detail,
            "soulseek_username": row.get("soulseek_username")
                or vr.soulseek_username,
            "source_dirs": source_dirs_from_validation_result(vr),
            "candidate": target,
            "local_items": vr.items,
            "import_job": import_job,
            "spectral_grade": row.get("spectral_grade"),
            "spectral_bitrate": row.get("spectral_bitrate"),
            "v0_probe_kind": row.get("v0_probe_kind"),
            "v0_probe_avg_bitrate": row.get("v0_probe_avg_bitrate"),
            "source_codec": row.get("evidence_source_codec"),
            "source_container": row.get("evidence_source_container"),
            "target_format": evidence_contract,
            "quality_lineage_version": evidence_lineage_version,
            "format": evidence_format
                if isinstance(evidence_format, str) else None,
            "min_bitrate": evidence_min_bitrate
                if isinstance(evidence_min_bitrate, int) else None,
            "avg_bitrate": evidence_avg_bitrate
                if isinstance(evidence_avg_bitrate, int) else None,
            "verified_lossless": bool(row.get("evidence_verified_lossless")),
            "quality_rank": entry_quality_rank,
        })
        group["pending_count"] = len(entries_list)

    # Enrich each group with a summary of the last successful import for the
    # request. Reuses the existing batch helper — returns newest-first per
    # request — and filters for active success/force_import plus historical
    # manual_import so the
    # header describes what's on disk rather than the latest attempt.
    if order:
        history = pdb.get_download_history_batch(order)
        for rid in order:
            rows_for_req = history.get(rid) or []
            groups[rid]["latest_import"] = _latest_import_summary(rows_for_req)

    # Sort entries within each group best-quality first so the operator
    # sees the most promising candidate (e.g. a FLAC) before the worse
    # ones (MP3 192). Ties broken by distance ascending then download_log
    # id descending (newest first).
    for entries_list in group_entries.values():
        entries_list.sort(key=_entry_sort_key)

    return [groups[rid] for rid in order]


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
    h._json({"groups": _build_wrong_match_groups(
        include_replaced=include_replaced)})


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

    entry = _server()._db().get_download_log_entry(log_id)
    if not entry:
        h._error(f"Download log entry {log_id} not found", 404)
        return

    try:
        payload = build_wrong_match_explorer(
            download_log_id=log_id,
            entry=entry,
        )
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
    h: _StreamingRouteHandler, params: dict[str, list[str]],
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

    entry = _server()._db().get_download_log_entry(log_id)
    if not entry:
        h._error(f"Download log entry {log_id} not found", 404)
        return

    try:
        abs_path, mime_type = resolve_wrong_match_stream_file(
            entry=entry,
            relative_path=relative_path,
        )
    except ValueError as exc:
        h._error(str(exc))
        return
    except FileNotFoundError as exc:
        h._error(str(exc), 404)
        return

    size = os.path.getsize(abs_path)
    try:
        requested_range = _byte_range(h.headers.get("Range"), size)
    except ValueError:
        h.send_response(416)
        h.send_header("Content-Range", f"bytes */{size}")
        h.send_header("Access-Control-Allow-Origin", "*")
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
    h.send_header("Access-Control-Allow-Origin", "*")
    if requested_range is not None:
        h.send_header("Content-Range", f"bytes {start}-{end}/{size}")
    h.end_headers()

    with open(abs_path, "rb") as handle:
        handle.seek(start)
        remaining = content_length
        while remaining > 0:
            chunk = handle.read(min(64 * 1024, remaining))
            if not chunk:
                break
            h.wfile.write(chunk)
            remaining -= len(chunk)
    if remaining > 0:
        # Short read (file truncated mid-stream): the body is shorter
        # than the declared Content-Length, so the keep-alive stream is
        # desynced — never reuse this socket.
        h.close_connection = True


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

    result = delete_wrong_match(_server()._db(), log_id, require_visible=True)
    if result.success:
        h._json({"status": "ok", **result.to_dict()})
        return
    if result.outcome == DELETE_OUTCOME_ACTIVE_JOB:
        h._error("active_import_job", 409)
        return
    if result.outcome == DELETE_OUTCOME_LOCKED:
        h._error(result.reason or result.outcome, 503)
        return
    if result.outcome in (DELETE_OUTCOME_INVALID_ROW, DELETE_OUTCOME_NOT_VISIBLE):
        h._error(result.reason or result.outcome, 404)
        return
    if result.outcome == DELETE_OUTCOME_UNSAFE_PATH:
        h._error(result.reason or result.outcome, 422)
        return
    h._error(result.error or result.reason or result.outcome, 500)


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

    summary = delete_wrong_match_group(_server()._db(), request_id)
    status = "ok" if summary.success else "partial"
    h._json(
        {"status": status, **summary.to_dict()},
        status=_wrong_match_delete_group_http_status(summary),
    )


def _wrong_match_delete_group_http_status(summary: WrongMatchDeleteSummary) -> int:
    """Mirror the CLI status/exit-code precedence for group delete."""
    if summary.success:
        return 200
    outcomes = {result.outcome for result in summary.results}
    if DELETE_OUTCOME_FAILED in outcomes:
        return 500
    if DELETE_OUTCOME_LOCKED in outcomes:
        return 503
    if DELETE_OUTCOME_ACTIVE_JOB in outcomes:
        return 409
    if DELETE_OUTCOME_UNSAFE_PATH in outcomes:
        return 422
    if outcomes & {DELETE_OUTCOME_INVALID_ROW, DELETE_OUTCOME_NOT_VISIBLE}:
        return 404
    return 409


class _GreenCandidate(TypedDict):
    """One green-distance force-import candidate assembled below."""

    download_log_id: int
    distance: float | None
    failed_path: str
    source_username: object
    source_dirs: list[str]


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

    srv = _server()
    pdb = srv._db()
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
        failed_path = vr.failed_path or ""
        distance = vr.distance
        green = _is_green_distance(vr, threshold_milli)

        if green:
            resolved_path = resolve_failed_path(failed_path)
            if resolved_path is None:
                skipped.append({
                    "download_log_id": lid,
                    "reason": "files_missing",
                })
                remaining += 1
                continue
            green_candidates.append({
                "download_log_id": lid,
                "distance": distance,
                "failed_path": resolved_path,
                "source_username": (
                    row.get("soulseek_username")
                    or vr.soulseek_username
                ),
                "source_dirs": source_dirs_from_validation_result(vr),
            })
            continue

        unmatched.append({
            "download_log_id": lid,
            "distance": distance,
        })
        unmatched_log_ids.append(lid)

    for candidate in green_candidates:
        lid = candidate["download_log_id"]
        source_username_raw = candidate.get("source_username")
        source_username = (
            str(source_username_raw)
            if source_username_raw is not None else None
        )
        job = pdb.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=rid,
            dedupe_key=force_import_dedupe_key(lid),
            payload=force_import_payload(
                download_log_id=lid,
                failed_path=str(candidate["failed_path"]),
                source_username=source_username,
                source_dirs=candidate["source_dirs"],
            ),
            message=(
                f"Force import queued for "
                f"{req['artist_name']} - {req['album_title']}"
            ),
        )
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

    if green_candidates:
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


def _preview_values_from_body(body: dict[str, object]) -> ImportPreviewValues:
    raw_values = body.get("values")
    if raw_values is None and body.get("values_json"):
        raw_values = json.loads(str(body["values_json"]))
    if raw_values is None:
        raw_values = body
    if not isinstance(raw_values, dict):
        raise ValueError("values must be an object")
    return msgspec.convert(raw_values, type=ImportPreviewValues)


def post_import_preview(h: RouteHandler, body: dict[str, object]) -> None:
    """Preview either typed values, a request/path, or a download-log row."""
    has_values = any(k in body for k in ("values", "values_json", "is_flac", "min_bitrate"))
    has_download_log = body.get("download_log_id") is not None
    has_path = body.get("request_id") is not None and body.get("path")
    mode_count = sum(1 for value in (has_values, has_download_log, has_path) if value)
    if mode_count != 1:
        h._error("Provide exactly one preview mode: values, download_log_id, or request_id+path")
        return

    try:
        if has_values:
            from lib.config import read_runtime_rank_config
            preview = preview_import_from_values(
                _preview_values_from_body(body),
                cfg=read_runtime_rank_config(),
            )
        elif has_download_log:
            download_log_db = _server()._db()
            raw_download_log_id = body["download_log_id"]
            if not isinstance(raw_download_log_id, (str, int, float)):
                raise TypeError("download_log_id must be a number or string")
            preview = preview_import_from_download_log(
                download_log_db,
                int(raw_download_log_id),
            )
        else:
            path_db = _server()._db()
            raw_request_id = body["request_id"]
            if not isinstance(raw_request_id, (str, int, float)):
                raise TypeError("request_id must be a number or string")
            preview = preview_import_from_path(
                path_db,
                request_id=int(raw_request_id),
                path=str(body["path"]),
                force=bool(body.get("force", True)),
            )
    except (ValueError, TypeError, msgspec.ValidationError) as exc:
        h._error(f"Invalid preview input: {exc}")
        return
    h._json(preview.to_dict())


class WrongMatchTriageRequest(BaseModel):
    """Confirmation guard so the destructive triage isn't accidental.

    ``confirm_all_wrong_matches`` must be the literal ``True``; Pydantic
    enforces type-and-value via a model validator (a plain ``bool``
    default would let ``False`` through).
    """

    confirm_all_wrong_matches: bool

    @model_validator(mode="after")
    def _must_be_true(self) -> "WrongMatchTriageRequest":
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
    duration. The sweep thread opens its own DB connection via
    ``_server()._new_db`` — psycopg2 handles are not thread-safe to
    share with the request thread.
    """
    req_body = parse_body(h, body, WrongMatchTriageRequest)
    if req_body is None:
        return
    started = _triage_runner.start(
        db_factory=_server()._new_db,
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
        "Preview whether an import would pass — accepts typed values, "
        "a download_log_id, or a request_id+path.",
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
]
