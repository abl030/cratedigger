"""Action-time album-quality evidence acquisition.

Preview creates durable candidate evidence; import and cleanup actions consume
it. This module keeps the action-facing "reuse or fail closed" provenance in
one place so callers do not accidentally treat legacy preview state or scalar
columns as mutation authority.
"""

from __future__ import annotations

import logging
from typing import Any, Callable

import msgspec

from lib.quality import (
    LOSSLESS_CODECS,
    V0_SOURCE_LINEAGE_LOSSLESS_SOURCE,
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
    fail_closed: bool = False


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

    request_row = db.get_request(request_id)
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
            request_row,
        )
        errors = _current_action_incomplete_reasons(
            existing,
            request_row,
            require_lossless_source_v0=existing_requires_lossless_source_v0,
        )
        snapshot_matches = (
            current_album_path is None
            or audio_snapshot_matches(current_album_path, existing.files)
        )
        if not errors and snapshot_matches:
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
            and not existing_snapshot_stale
            and not _request_has_current_lossless_source_v0(request_row)
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
                fail_closed=True,
            ),
        )

    if backfilled.evidence is not None:
        backfilled_errors = _current_action_incomplete_reasons(
            backfilled.evidence,
            request_row,
            require_lossless_source_v0=(
                existing_requires_lossless_source_v0
                and not existing_snapshot_stale
            ),
        )
        if backfilled_errors:
            backfilled = EvidenceBuildResult(
                None,
                "incomplete",
                "; ".join(backfilled_errors),
            )
        else:
            if backfill_builder is not None:
                db.upsert_album_quality_evidence(backfilled.evidence)
            return CurrentEvidenceActionResult(
                evidence=backfilled.evidence,
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
            fail_closed=True,
        ),
    )


def _current_action_incomplete_reasons(
    evidence: AlbumQualityEvidence,
    request_row: dict[str, Any] | None,
    *,
    require_lossless_source_v0: bool = False,
) -> list[str]:
    reasons = current_evidence_rebuild_reasons(evidence)
    if (
        (require_lossless_source_v0 or _requires_lossless_source_v0_metric(
            evidence,
            request_row,
        ))
        and not _has_lossless_source_v0_metric(evidence)
    ):
        reasons.append(
            "lossless-source V0 metric is required for converted current evidence"
        )
    return reasons


def _requires_lossless_source_v0_metric(
    evidence: AlbumQualityEvidence,
    request_row: dict[str, Any] | None,
) -> bool:
    if _request_has_current_lossless_source_v0(request_row):
        return True
    converted_from = (evidence.measurement.was_converted_from or "").lower()
    return converted_from in LOSSLESS_CODECS


def _request_has_current_lossless_source_v0(
    request_row: dict[str, Any] | None,
) -> bool:
    if request_row is None:
        return False
    return any(
        request_row.get(field) is not None
        for field in (
            "current_lossless_source_v0_probe_min_bitrate",
            "current_lossless_source_v0_probe_avg_bitrate",
            "current_lossless_source_v0_probe_median_bitrate",
        )
    )


def _has_lossless_source_v0_metric(evidence: AlbumQualityEvidence) -> bool:
    metric = evidence.v0_metric
    return (
        metric is not None
        and metric.source_lineage == V0_SOURCE_LINEAGE_LOSSLESS_SOURCE
        and metric.avg_bitrate_kbps is not None
    )


def load_current_evidence_for_action(
    db: QualityEvidenceDB,
    *,
    request_id: int,
    mb_release_id: str,
    quality_ranks: QualityRankConfig | None = None,
    beets_library_root: str = "",
) -> CurrentEvidenceActionResult | None:
    """Look Beets up by MBID then load/backfill; return None if no album, fail-closed on error."""

    cfg = quality_ranks if quality_ranks is not None else QualityRankConfig.defaults()
    try:
        from lib.beets_db import BeetsDB

        with BeetsDB(library_root=beets_library_root) as beets:
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
    if status in {"missing", "empty_current", "empty_fileset"}:
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
