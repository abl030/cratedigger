"""Action-time album-quality evidence acquisition.

Preview creates durable candidate evidence; import and cleanup actions consume
it. This module keeps the action-facing "reuse or fail closed" provenance in
one place so callers do not accidentally treat legacy preview state or scalar
columns as mutation authority.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Callable, Literal

import msgspec

from lib.quality import (
    LOSSLESS_CODECS,
    EVIDENCE_SUBJECT_SOURCE,
    AlbumQualityEvidence,
    QualityRankConfig,
)
from lib.quality_evidence import (
    EvidenceBuildResult,
    QualityEvidenceDB,
    audio_snapshot_matches,
    backfill_current_evidence_from_album_info,
    current_evidence_rebuild_reasons,
    load_candidate_evidence_for_source,
    load_or_backfill_current_evidence,
)

logger = logging.getLogger("cratedigger")


CANDIDATE_STATUS_REUSED = "reused"
CANDIDATE_STATUS_MISSING = "missing"
CANDIDATE_STATUS_STALE = "stale"
CANDIDATE_STATUS_INCOMPLETE = "incomplete"
CANDIDATE_STATUS_FAILED = "failed"

CURRENT_STATUS_LOADED = "loaded"
CURRENT_STATUS_BACKFILLED = "backfilled"
CURRENT_STATUS_MISSING = "missing"
CURRENT_STATUS_FAILED = "failed"

SNAPSHOT_GUARD_MATCHED = "matched"
SNAPSHOT_GUARD_MISSING = "missing"
SNAPSHOT_GUARD_STALE = "stale"
SNAPSHOT_GUARD_NOT_CHECKED = "not_checked"
SNAPSHOT_GUARD_FAILED = "failed"


CurrentEvidenceBackfillBuilder = Callable[..., EvidenceBuildResult]

__all__ = [
    "ActionEvidenceProvenance",
    "CandidateEvidenceActionResult",
    "CurrentEvidenceActionResult",
    "HaveAnalysisFailure",
    "classify_have_analysis_failure",
    "ensure_candidate_evidence_for_action",
    "ensure_current_evidence_for_action",
    "load_current_evidence_for_action",
]


class ActionEvidenceProvenance(msgspec.Struct, frozen=True):
    """Provenance summary suitable for import/cleanup action results."""

    candidate_status: str | None = None
    current_status: str | None = None
    snapshot_guard: str = SNAPSHOT_GUARD_NOT_CHECKED
    fallback_reason: str | None = None
    installed_path: str | None = None
    fail_closed: bool = False


HaveAnalysisFailureCategory = Literal[
    "permission_denied",
    "path_missing",
    "no_audio_files",
    "snapshot_changed",
    "analyser_failure",
]


class HaveAnalysisFailure(msgspec.Struct, frozen=True):
    """Typed audit payload for a failed installed-HAVE analysis."""

    failure_category: HaveAnalysisFailureCategory
    error: str
    installed_path: str | None = None
    candidate_reference: str | None = None


def classify_have_analysis_failure(
    error: str,
    *,
    snapshot_guard: str | None = None,
) -> HaveAnalysisFailureCategory:
    """Map raw evidence-acquisition diagnostics to the operator taxonomy."""

    normalized = error.casefold()
    if snapshot_guard == SNAPSHOT_GUARD_STALE or "snapshot" in normalized:
        return "snapshot_changed"
    if any(token in normalized for token in (
        "no audio", "empty_fileset", "empty current", "zero audio",
    )):
        return "no_audio_files"
    if any(token in normalized for token in (
        "permission denied", "permissionerror", "eacces",
    )):
        return "permission_denied"
    if any(token in normalized for token in (
        "no such file", "filenotfounderror", "path not found",
        "source vanished", "missing path",
    )):
        return "path_missing"
    return "analyser_failure"


class CandidateEvidenceActionResult(msgspec.Struct, frozen=True):
    evidence: AlbumQualityEvidence | None
    provenance: ActionEvidenceProvenance

    @property
    def available(self) -> bool:
        return self.evidence is not None and not self.provenance.fail_closed


class CurrentEvidenceActionResult(msgspec.Struct, frozen=True):
    evidence: AlbumQualityEvidence | None
    provenance: ActionEvidenceProvenance

    @property
    def available(self) -> bool:
        return self.evidence is not None and not self.provenance.fail_closed


def ensure_candidate_evidence_for_action(
    db: QualityEvidenceDB,
    *,
    source_path: str,
    download_log_id: int | None = None,
    import_job_id: int | None = None,
) -> CandidateEvidenceActionResult:
    """Load valid candidate evidence for a mutating action or fail closed."""

    loaded = load_candidate_evidence_for_source(
        db,
        source_path=source_path,
        download_log_id=download_log_id,
        import_job_id=import_job_id,
    )
    if loaded.evidence is not None:
        return CandidateEvidenceActionResult(
            evidence=loaded.evidence,
            provenance=ActionEvidenceProvenance(
                candidate_status=CANDIDATE_STATUS_REUSED,
                snapshot_guard=SNAPSHOT_GUARD_MATCHED,
            ),
        )

    snapshot_guard = _snapshot_guard_from_candidate_status(loaded.status)
    return CandidateEvidenceActionResult(
        evidence=None,
        provenance=ActionEvidenceProvenance(
            candidate_status=_candidate_action_status(loaded.status),
            snapshot_guard=snapshot_guard,
            fallback_reason=loaded.reason,
            fail_closed=True,
        ),
    )


def ensure_current_evidence_for_action(
    db: QualityEvidenceDB,
    *,
    request_id: int,
    mb_release_id: str,
    quality_ranks: Any = None,
    current_album_path: str | None = None,
    album_info: Any = None,
    backfill_builder: CurrentEvidenceBackfillBuilder | None = None,
    beets_library_root: str = "",
) -> CurrentEvidenceActionResult:
    """Load or backfill current Beets evidence with action provenance."""

    existing_id = db.get_request_current_evidence_id(request_id)
    existing = (
        db.load_album_quality_evidence_by_id(existing_id)
        if existing_id is not None
        else None
    )
    existing_snapshot_stale = False
    existing_requires_lossless_source_v0 = False
    if existing is not None:
        existing_requires_lossless_source_v0 = _requires_lossless_source_v0_metric(
            existing,
        )
        errors = _current_action_incomplete_reasons(
            existing,
            require_lossless_source_v0=existing_requires_lossless_source_v0,
        )
        snapshot_matches = (
            current_album_path is None
            or audio_snapshot_matches(current_album_path, existing.files)
        )
        if not errors and snapshot_matches:
            # Candidate and installed snapshots can have the same content
            # address.  A later candidate upsert then reuses the installed
            # row and writes its disposable staging path into ``source_path``.
            # The linked current row must prefer the durable Beets path: all
            # enrichment helpers use this field as their read boundary after
            # the candidate directory has been cleaned up.
            if (
                current_album_path is not None
                and os.path.realpath(existing.source_path)
                != os.path.realpath(current_album_path)
            ):
                existing = msgspec.structs.replace(
                    existing,
                    source_path=current_album_path,
                )
                db.upsert_album_quality_evidence(existing)
            return CurrentEvidenceActionResult(
                evidence=existing,
                provenance=ActionEvidenceProvenance(
                    current_status=CURRENT_STATUS_LOADED,
                    snapshot_guard=(
                        SNAPSHOT_GUARD_MATCHED
                        if current_album_path is not None
                        else SNAPSHOT_GUARD_NOT_CHECKED
                    ),
                    ),
                )
        existing_snapshot_stale = (
            current_album_path is not None and not snapshot_matches
        )
        fallback_reason = (
            "; ".join(errors)
            if errors
            else "current album files changed since evidence capture"
        )
        if (
            existing_requires_lossless_source_v0
            and not _has_lossless_source_v0_metric(existing)
        ):
            return CurrentEvidenceActionResult(
                evidence=None,
                provenance=ActionEvidenceProvenance(
                    current_status=CURRENT_STATUS_FAILED,
                    snapshot_guard=(
                        SNAPSHOT_GUARD_MATCHED
                        if current_album_path is not None
                        else SNAPSHOT_GUARD_NOT_CHECKED
                    ),
                    fallback_reason=fallback_reason,
                    installed_path=(
                        current_album_path
                        or (existing.source_path if existing is not None else None)
                    ),
                    fail_closed=True,
                ),
            )
    else:
        fallback_reason = "no current evidence found"

    try:
        if backfill_builder is not None:
            backfilled = backfill_builder(
                db,
                request_id=request_id,
                mb_release_id=mb_release_id,
                quality_ranks=quality_ranks,
                album_info=album_info,
            )
        elif album_info is not None:
            backfilled = backfill_current_evidence_from_album_info(
                db,
                request_id=request_id,
                mb_release_id=mb_release_id,
                album_info=album_info,
            )
        else:
            backfilled = load_or_backfill_current_evidence(
                db,
                request_id=request_id,
                mb_release_id=mb_release_id,
                quality_ranks=quality_ranks,
                preloaded_evidence=None if existing_snapshot_stale else existing,
                preloaded=True,
                beets_library_root=beets_library_root,
            )
    except Exception as exc:
        return CurrentEvidenceActionResult(
            evidence=None,
            provenance=ActionEvidenceProvenance(
                current_status=CURRENT_STATUS_FAILED,
                snapshot_guard=(
                    SNAPSHOT_GUARD_STALE
                    if existing is not None and current_album_path is not None
                    else SNAPSHOT_GUARD_NOT_CHECKED
                ),
                fallback_reason=f"{type(exc).__name__}: {exc}",
                installed_path=(
                    current_album_path
                    or (existing.source_path if existing is not None else None)
                ),
                fail_closed=True,
            ),
        )

    if backfilled.evidence is not None and backfilled.status == "ready":
        authoritative = backfilled.evidence
        try:
            linked_id = db.get_request_current_evidence_id(request_id)
            linked = (
                db.load_album_quality_evidence_by_id(linked_id)
                if linked_id is not None
                else None
            )
        except Exception as exc:
            linked = None
            link_error = f"{type(exc).__name__}: {exc}"
        else:
            link_error = None
        if (
            linked is None
            or linked.id is None
            or linked.mb_release_id != backfilled.evidence.mb_release_id
            or linked.snapshot_fingerprint
                != backfilled.evidence.snapshot_fingerprint
        ):
            backfilled = EvidenceBuildResult(
                None,
                "incomplete",
                link_error or "backfilled evidence is not the exact linked snapshot",
            )
        else:
            authoritative = linked
            backfilled_errors = _current_action_incomplete_reasons(
                authoritative,
                require_lossless_source_v0=(
                    existing_requires_lossless_source_v0
                ),
            )
            if existing_snapshot_stale:
                backfilled_errors.extend(
                    _current_action_missing_enrichment_reasons(authoritative)
                )
            if backfilled_errors:
                backfilled = EvidenceBuildResult(
                    None,
                    "incomplete",
                    "; ".join(backfilled_errors),
                )
            else:
                return CurrentEvidenceActionResult(
                    evidence=authoritative,
                    provenance=ActionEvidenceProvenance(
                        current_status=CURRENT_STATUS_BACKFILLED,
                        snapshot_guard=SNAPSHOT_GUARD_MATCHED,
                        fallback_reason=fallback_reason,
                    ),
                )

    return CurrentEvidenceActionResult(
        evidence=None,
        provenance=ActionEvidenceProvenance(
            current_status=_current_action_status(backfilled.status),
            snapshot_guard=(
                SNAPSHOT_GUARD_STALE
                if existing is not None and current_album_path is not None
                else SNAPSHOT_GUARD_NOT_CHECKED
            ),
            fallback_reason=backfilled.reason or fallback_reason,
            installed_path=(
                current_album_path
                or (existing.source_path if existing is not None else None)
            ),
            fail_closed=True,
        ),
    )


def _current_action_incomplete_reasons(
    evidence: AlbumQualityEvidence,
    *,
    require_lossless_source_v0: bool = False,
) -> list[str]:
    reasons = current_evidence_rebuild_reasons(evidence)
    if (
        (require_lossless_source_v0 or _requires_lossless_source_v0_metric(evidence))
        and not _has_lossless_source_v0_metric(evidence)
    ):
        reasons.append(
            "lossless-source V0 metric is required for converted current evidence"
        )
    return reasons


def _current_action_missing_enrichment_reasons(
    evidence: AlbumQualityEvidence,
) -> list[str]:
    """Require neutral installed facts before a newly changed snapshot acts."""

    reasons: list[str] = []
    measurement = evidence.measurement
    if (
        measurement.spectral_grade is None
        and measurement.spectral_bitrate_kbps is None
    ):
        reasons.append("exact current snapshot still needs installed spectral enrichment")
    if (
        evidence.v0_metric is None
        and not evidence.on_disk_v0_research_attempted
    ):
        reasons.append("exact current snapshot still needs installed V0 enrichment")
    return reasons


def _requires_lossless_source_v0_metric(
    evidence: AlbumQualityEvidence,
) -> bool:
    converted_from = (evidence.measurement.was_converted_from or "").lower()
    return converted_from in LOSSLESS_CODECS


def _has_lossless_source_v0_metric(evidence: AlbumQualityEvidence) -> bool:
    metric = evidence.v0_metric
    return (
        metric is not None
        and metric.subject == EVIDENCE_SUBJECT_SOURCE
        and metric.avg_bitrate_kbps is not None
    )


def load_current_evidence_for_action(
    db: QualityEvidenceDB,
    *,
    request_id: int,
    mb_release_id: str,
    quality_ranks: QualityRankConfig | None = None,
    beets_library_db_path: str | None = None,
    beets_library_root: str = "",
) -> CurrentEvidenceActionResult | None:
    """Look Beets up by MBID then load/backfill; return None if no album, fail-closed on error."""

    cfg = quality_ranks if quality_ranks is not None else QualityRankConfig.defaults()
    try:
        from lib.beets_db import BeetsDB

        if beets_library_db_path is None:
            beets_handle = BeetsDB(library_root=beets_library_root)
        else:
            beets_handle = BeetsDB(
                beets_library_db_path,
                library_root=beets_library_root,
            )
        with beets_handle as beets:
            album_info = beets.get_album_info(mb_release_id, cfg)
        if album_info is None:
            return None
        return ensure_current_evidence_for_action(
            db,
            request_id=request_id,
            mb_release_id=mb_release_id,
            quality_ranks=cfg,
            current_album_path=album_info.album_path,
            album_info=album_info,
            beets_library_root=beets_library_root,
        )
    except Exception as exc:
        logger.debug(
            "Failed to load/backfill current quality evidence for request %s",
            request_id,
            exc_info=True,
        )
        return CurrentEvidenceActionResult(
            evidence=None,
            provenance=ActionEvidenceProvenance(
                current_status=CURRENT_STATUS_FAILED,
                fallback_reason=f"{type(exc).__name__}: {exc}",
                fail_closed=True,
            ),
        )

def _candidate_action_status(status: str) -> str:
    if status in {"missing", "unowned", "empty_fileset"}:
        return CANDIDATE_STATUS_MISSING
    if status == "stale":
        return CANDIDATE_STATUS_STALE
    if status == "incomplete":
        return CANDIDATE_STATUS_INCOMPLETE
    return CANDIDATE_STATUS_FAILED


def _current_action_status(status: str) -> str:
    if status in {"missing", "empty_current"}:
        return CURRENT_STATUS_MISSING
    return CURRENT_STATUS_FAILED


def _snapshot_guard_from_candidate_status(status: str) -> str:
    if status == "ready":
        return SNAPSHOT_GUARD_MATCHED
    if status == "stale":
        return SNAPSHOT_GUARD_STALE
    if status in {"missing", "unowned", "empty_fileset"}:
        return SNAPSHOT_GUARD_MISSING
    if status == "failed":
        return SNAPSHOT_GUARD_FAILED
    return SNAPSHOT_GUARD_MATCHED
