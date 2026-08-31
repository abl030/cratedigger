"""Wrong Matches queue projection — the grouped payload behind /api/wrong-matches.

Extracted from ``web/routes/imports.py`` (#1278) so the projection has a
non-HTTP interface: ``build_wrong_match_groups`` takes its collaborators
explicitly — the pipeline-DB reads as a narrow Protocol, the batched beets
lookup, and the rank producer — instead of reaching into the web server
singleton. The route is a thin adapter that supplies the production trio
(``srv._db()``, ``srv.check_beets_library_detail``,
``srv.compute_library_rank``); the latter two stay looked up on
``web.server`` at request time so that module remains the single
interception seam for both.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
from typing import Protocol

from lib.import_queue import ForceImportPayload, ImportJob, YoutubeImportPayload
from lib.pipeline_db._shared import (
    CANDIDATE_EVIDENCE_PREFIX,
    CURRENT_EVIDENCE_PREFIX,
)
from lib.pipeline_db.rows import (
    DownloadLogWithEvidenceRow,
    WrongMatchCandidateRow,
)
from lib.quality import _is_explicit_label
from lib.util import observe_failed_path
from lib.validation_envelope import decode_validation_envelope
from lib.wrong_matches import wrong_match_row_is_visible
from web.classify import (
    average_quality_label,
    evidence_column_accusation_flags,
)
from web.wrong_match_file_service import (
    source_dirs_from_validation_result,
    target_candidate,
)


class WrongMatchQueueDB(Protocol):
    """The pipeline-DB reads the queue projection performs.

    ``PipelineDB`` and ``FakePipelineDB`` both satisfy this structurally.
    """

    def get_wrong_matches(self) -> list[WrongMatchCandidateRow]: ...

    def list_active_import_jobs(
        self, *, request_id: int | None = None, limit: int = 50,
    ) -> list[ImportJob]: ...

    def get_download_history_batch(
        self, request_ids: list[int],
    ) -> dict[int, list[DownloadLogWithEvidenceRow]]: ...


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


# Numeric rank for the per-entry sort. Higher = better quality. Mirrors
# QualityRank's integer ordering but keeps the view layer free of an
# enum import; quality_rank strings here come from the injected
# compute_library_rank, which the route supplies from web.server —
# still the single production producer.
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
                     compute_library_rank: Callable[[str | None, int | None], str],
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
            "current_spectral_accusation_admissible": None,
            "current_spectral_accusation_withheld": None,
            "quality_label": None,
            "quality_rank": None,
        }

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
            label = average_quality_label(fmt, beets_avg_kbps)
        rank = compute_library_rank(fmt, beets_avg_kbps)

    # The badge renders ``album_requests.current_spectral_grade``; these
    # two flags are re-derived from the request's linked current evidence
    # so the badge stops accusing an audit-only codec of transcoding
    # (issue #829 Phase 5 PR4). Absent evidence leaves both None and the
    # badge keeps its historical accusing form.
    have_flags = evidence_column_accusation_flags(
        row, prefix=CURRENT_EVIDENCE_PREFIX)
    return {
        "status": status,
        "min_bitrate": beets_min_kbps if beets_min_kbps is not None else db_kbps,
        "avg_bitrate": beets_avg_kbps,
        "format": fmt,
        "verified_lossless": bool(row.get("request_verified_lossless") or False),
        "current_spectral_grade": row.get("request_current_spectral_grade"),
        "current_spectral_bitrate": row.get("request_current_spectral_bitrate"),
        "current_spectral_accusation_admissible": have_flags.admissible,
        "current_spectral_accusation_withheld": have_flags.withheld,
        "quality_label": label,
        "quality_rank": rank,
    }


_IMPORT_SUCCESS_OUTCOMES = (
    "success", "force_import", "local_import", "manual_import",
)


def _latest_import_summary(rows: Sequence[Mapping[str, object]]
                           ) -> dict[str, object] | None:
    """Summary of the last successful import for a request.

    The expanded-group header describes what's currently on disk, not the most
    recent attempt. A rejection that happened after a successful import
    doesn't change what beets has — the earlier success is still the
    authoritative picture. Scan the newest-first history for the first
    active success/force_import/local_import row or historical manual_import
    row and surface its metadata.

    Returns ``None`` when the release has never been successfully imported.
    """
    if not rows:
        return None
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


def build_wrong_match_groups(
    *,
    db: WrongMatchQueueDB,
    check_beets_library_detail: Callable[
        [list[str]], dict[str, dict[str, object]],
    ],
    compute_library_rank: Callable[[str | None, int | None], str],
    include_replaced: bool = False,
) -> list[dict[str, object]]:
    """Group wrong-match rejections by release (issue #113).

    Each ``album_requests`` row becomes one group; a rejected
    ``download_log`` entry becomes an entry inside its group when its
    ``failed_path`` is either present on disk or UNOBSERVABLE. Only entries
    whose folder was PROVEN absent are dropped, so a group can be dropped
    for having no surviving entries while an entry that merely could not be
    read stays visible, flagged ``path_unavailable`` (issue #1063 — silently
    dropping those hid the broken world from its only operator surface).

    Each group also carries an on-disk quality snapshot (format, bitrate,
    verified_lossless, spectral grade, rank tier) and the most-recent
    ``download_log`` row for the request, so the user can judge at a glance
    whether it's worth trying to force-import a rejected candidate.
    """
    rows = db.get_wrong_matches()
    active_import_jobs = db.list_active_import_jobs(limit=200)
    active_jobs_by_log_id: dict[int, ImportJob] = {}
    active_jobs_by_request_id: dict[int, list[ImportJob]] = {}
    for job in active_import_jobs:
        request_id = job.request_id
        if isinstance(request_id, int):
            active_jobs_by_request_id.setdefault(request_id, []).append(job)
        if isinstance(job.payload, (ForceImportPayload, YoutubeImportPayload)):
            active_jobs_by_log_id[job.payload.download_log_id] = job
    mbids = [
        mbid for row in rows
        for mbid in [row.get("mb_release_id")]
        if isinstance(mbid, str) and mbid
    ]
    beets_info = check_beets_library_detail(mbids) if mbids else {}

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
        observation = observe_failed_path(failed_path)
        resolved_path = observation.path
        files_exist = observation.present
        # A folder we PROVED is gone leaves the worklist; one we merely
        # could not read stays, flagged. Dropping it silently hid the
        # broken world from the only surface that could report it (#1063).
        if not files_exist and not observation.indeterminate:
            continue
        path_unavailable = observation.indeterminate

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
                    job.to_json_dict()
                    for job in active_jobs_by_request_id.get(request_id, [])
                ],
                "latest_import": None,  # filled in after the loop
                **_quality_summary(
                    row, beets_info, presence, compute_library_rank,
                ),
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
            active_jobs_by_log_id[log_id].to_json_dict()
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
        entry_quality_rank = compute_library_rank(
            evidence_format if isinstance(evidence_format, str) else None,
            evidence_avg_bitrate if isinstance(evidence_avg_bitrate, int) else None,
        )
        # The per-entry chip renders the candidate's OWN grade, so its
        # audit-only flags come from the candidate evidence join, never
        # the request's installed copy (issue #829 Phase 5 PR4).
        candidate_flags = evidence_column_accusation_flags(
            row, prefix=CANDIDATE_EVIDENCE_PREFIX)
        entries_list.append({
            "download_log_id": log_id,
            "failed_path": resolved_path or failed_path,
            "files_exist": files_exist,
            "path_unavailable": path_unavailable,
            "path_unavailable_reason": (
                observation.unavailable_reason() if path_unavailable else None
            ),
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
            "spectral_accusation_admissible": candidate_flags.admissible,
            "spectral_accusation_withheld": candidate_flags.withheld,
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
        history = db.get_download_history_batch(order)
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
