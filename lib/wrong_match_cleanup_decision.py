"""Action-time cleanup authorization for Wrong Matches."""

from __future__ import annotations

import logging
from typing import Any, Callable

import msgspec

from lib.import_preview import ImportPreviewResult
from lib.import_evidence import (
    ensure_candidate_evidence_for_action,
    ensure_current_evidence_for_action,
)
from lib.quality import (
    AlbumQualityEvidenceDecisionFacts,
    classify_full_pipeline_decision,
    evidence_decision_name,
    full_pipeline_decision_from_evidence,
    ImportResult,
)
from lib.quality_evidence import (
    lossless_source_v0_probe_from_metric,
)
from lib.util import resolve_failed_path
from lib.wrong_matches import validation_failed_path

CLEANUP_DECISION_PROVENANCE = "album_quality_evidence"
logger = logging.getLogger("cratedigger")

PreviewBuilder = Callable[[Any, int], ImportPreviewResult]


class WrongMatchCleanupDecision(msgspec.Struct, frozen=True):
    """Fresh authorization result for destructive Wrong Matches cleanup."""

    download_log_id: int
    delete_allowed: bool
    uncertain: bool
    provenance: str
    verdict: str
    confident_reject: bool
    cleanup_eligible: bool
    preview_decision: str | None = None
    reason: str | None = None
    detail: str | None = None
    request_id: int | None = None
    source_path: str | None = None
    stage_chain: tuple[str, ...] = ()
    import_result: ImportResult | None = None

    @property
    def skip_reason(self) -> str:
        return (
            self.reason
            or self.preview_decision
            or self.detail
            or self.verdict
        )

    def to_dict(self) -> dict[str, object]:
        data = msgspec.to_builtins(self)
        data.pop("import_result", None)
        return data


def decide_wrong_match_cleanup(
    db: Any,
    download_log_id: int,
    *,
    preview_builder: PreviewBuilder | None = None,
    cfg: Any = None,
) -> WrongMatchCleanupDecision:
    """Recompute whether Wrong Matches cleanup may delete a source folder."""
    if cfg is None:
        try:
            from lib.config import read_runtime_config

            cfg = read_runtime_config()
        except Exception:
            logger.debug(
                "Failed to load runtime config for wrong-match cleanup",
                exc_info=True,
            )
            return _uncertain(download_log_id, "runtime_config_unavailable")

    entry = db.get_download_log_entry(download_log_id)
    if not entry:
        return _uncertain(download_log_id, "download_log_missing")

    request_id = entry.get("request_id")
    if not isinstance(request_id, int):
        return _uncertain(download_log_id, "request_id_missing")
    request = db.get_request(request_id)
    if not request:
        return _uncertain(download_log_id, "request_missing")

    failed_path = validation_failed_path(entry.get("validation_result"))
    if not failed_path:
        return _uncertain(download_log_id, "failed_path_missing")

    candidate_source_path = resolve_failed_path(failed_path) or failed_path
    candidate_result = ensure_candidate_evidence_for_action(
        db,
        source_path=candidate_source_path,
        download_log_id=download_log_id,
    )
    if not candidate_result.available:
        if preview_builder is None:
            return _uncertain(
                download_log_id,
                candidate_result.provenance.fallback_reason
                or "candidate_evidence_unavailable",
            )
        preview = preview_builder(db, download_log_id)
        candidate_source_path = preview.source_path or failed_path
        candidate_result = ensure_candidate_evidence_for_action(
            db,
            source_path=candidate_source_path,
            download_log_id=download_log_id,
        )
        if not candidate_result.available or candidate_result.evidence is None:
            return _uncertain(
                download_log_id,
                preview.reason
                or candidate_result.provenance.fallback_reason
                or candidate_result.provenance.candidate_status
                or "candidate_evidence_unavailable",
            )

    if candidate_result.evidence is None:
        return _uncertain(
            download_log_id,
            candidate_result.provenance.fallback_reason
            or "candidate_evidence_unavailable",
        )

    mb_release_id = str(request.get("mb_release_id") or "")
    try:
        from lib.beets_db import BeetsDB
        from lib.quality import QualityRankConfig

        quality_ranks = getattr(cfg, "quality_ranks", None)
        rank_cfg = (
            quality_ranks
            if quality_ranks is not None
            else QualityRankConfig.defaults()
        )
        with BeetsDB() as beets:
            album_info = beets.get_album_info(mb_release_id, rank_cfg)
    except Exception:
        logger.debug(
            "Failed to load current Beets album for wrong-match cleanup",
            exc_info=True,
        )
        return _uncertain(download_log_id, "current_evidence_unavailable")

    current_evidence = None
    current_status = "missing"
    if album_info is not None:
        current_result = ensure_current_evidence_for_action(
            db,
            request_id=request_id,
            mb_release_id=mb_release_id,
            quality_ranks=getattr(cfg, "quality_ranks", None),
            current_album_path=album_info.album_path,
            album_info=album_info,
        )
        if current_result.evidence is None:
            return _uncertain(
                download_log_id,
                current_result.provenance.fallback_reason
                or "current_evidence_unavailable",
            )
        current_evidence = current_result.evidence
        current_status = current_result.provenance.current_status or "unknown"

    decision_result = full_pipeline_decision_from_evidence(
        candidate_result.evidence,
        current_evidence,
        facts=AlbumQualityEvidenceDecisionFacts(
            import_mode="cleanup",
            verified_lossless_target=getattr(
                cfg,
                "verified_lossless_target",
                None,
            ),
            target_format=request.get("target_format"),
        ),
        cfg=getattr(cfg, "quality_ranks", None),
    )
    verdict, cleanup_eligible, reason = classify_full_pipeline_decision(
        decision_result
    )
    decision_name = evidence_decision_name(decision_result)
    confident_reject = verdict == "confident_reject"
    delete_allowed = confident_reject and cleanup_eligible
    return WrongMatchCleanupDecision(
        download_log_id=download_log_id,
        delete_allowed=delete_allowed,
        uncertain=False,
        provenance=CLEANUP_DECISION_PROVENANCE,
        verdict=verdict,
        confident_reject=confident_reject,
        cleanup_eligible=cleanup_eligible,
        preview_decision=decision_name,
        reason=reason,
        detail=(
            f"candidate_status={candidate_result.provenance.candidate_status}; "
            f"current_status={current_status}"
        ),
        request_id=request_id,
        source_path=candidate_source_path,
        stage_chain=_stage_chain_from_decision(decision_result),
        import_result=ImportResult(
            decision=decision_name,
            new_measurement=candidate_result.evidence.measurement,
            existing_measurement=(
                current_evidence.measurement
                if current_evidence is not None
                else None
            ),
            v0_probe=lossless_source_v0_probe_from_metric(
                candidate_result.evidence.v0_metric
            ),
            existing_v0_probe=lossless_source_v0_probe_from_metric(
                current_evidence.v0_metric if current_evidence is not None else None
            ),
        ),
    )


def _uncertain(download_log_id: int, reason: str) -> WrongMatchCleanupDecision:
    return WrongMatchCleanupDecision(
        download_log_id=download_log_id,
        delete_allowed=False,
        uncertain=True,
        provenance=CLEANUP_DECISION_PROVENANCE,
        verdict="uncertain",
        confident_reject=False,
        cleanup_eligible=False,
        reason=reason,
    )


def _stage_chain_from_decision(decision: dict[str, object]) -> tuple[str, ...]:
    chain: list[str] = []
    for key in (
        "preimport_nested",
        "preimport_audio",
        "stage0_spectral_gate",
        "stage1_spectral",
        "stage2_import",
        "stage3_quality_gate",
    ):
        value = decision.get(key)
        if value is not None:
            chain.append(f"{key}:{value}")
    return tuple(chain)
