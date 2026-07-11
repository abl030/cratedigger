"""Typed test double for the core import-dispatch callable."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Sequence

from lib.config import CratediggerConfig
from lib.dispatch import DispatchCoreFn, DispatchOutcome, QualityGateFn
from lib.dispatch.quality_gate import _check_quality_gate_core
from lib.import_evidence import CandidateEvidenceActionResult
from lib.pipeline_db import DownloadLogOutcome, PipelineDB
from lib.quality import DownloadInfo


@dataclass(frozen=True)
class DispatchCoreCall:
    path: str
    mb_release_id: str
    request_id: int
    label: str
    force: bool
    override_min_bitrate: int | None
    target_format: str | None
    verified_lossless_target: str
    beets_harness_path: str
    db: PipelineDB
    dl_info: DownloadInfo
    distance: float | None
    scenario: str
    files: Sequence[object] | None
    cfg: CratediggerConfig | None
    outcome_label: DownloadLogOutcome
    requeue_on_failure: bool
    cooled_down_users: set[str] | None
    source_dirs: list[str] | None
    candidate_import_job_id: int | None
    candidate_download_log_id: int | None
    prevalidated_candidate_result: CandidateEvidenceActionResult | None
    quality_gate_fn: QualityGateFn


@dataclass
class RecordingDispatchCore:
    """Record exact dispatch calls while returning a production-shaped result."""

    outcome: DispatchOutcome = field(default_factory=lambda: DispatchOutcome(
        success=True,
        message="recorded test dispatch",
    ))
    calls: list[DispatchCoreCall] = field(default_factory=list)

    def __call__(
        self,
        *,
        path: str,
        mb_release_id: str,
        request_id: int,
        label: str,
        force: bool = False,
        override_min_bitrate: int | None = None,
        target_format: str | None = None,
        verified_lossless_target: str = "",
        beets_harness_path: str,
        db: PipelineDB,
        dl_info: DownloadInfo,
        distance: float | None = None,
        scenario: str = "auto_import",
        files: Sequence[object] | None = None,
        cfg: CratediggerConfig | None = None,
        outcome_label: DownloadLogOutcome = "success",
        requeue_on_failure: bool = True,
        cooled_down_users: set[str] | None = None,
        source_dirs: list[str] | None = None,
        candidate_import_job_id: int | None = None,
        candidate_download_log_id: int | None = None,
        prevalidated_candidate_result: CandidateEvidenceActionResult | None = None,
        quality_gate_fn: QualityGateFn = _check_quality_gate_core,
    ) -> DispatchOutcome:
        self.calls.append(DispatchCoreCall(
            path=path,
            mb_release_id=mb_release_id,
            request_id=request_id,
            label=label,
            force=force,
            override_min_bitrate=override_min_bitrate,
            target_format=target_format,
            verified_lossless_target=verified_lossless_target,
            beets_harness_path=beets_harness_path,
            db=db,
            dl_info=dl_info,
            distance=distance,
            scenario=scenario,
            files=files,
            cfg=cfg,
            outcome_label=outcome_label,
            requeue_on_failure=requeue_on_failure,
            cooled_down_users=cooled_down_users,
            source_dirs=source_dirs,
            candidate_import_job_id=candidate_import_job_id,
            candidate_download_log_id=candidate_download_log_id,
            prevalidated_candidate_result=prevalidated_candidate_result,
            quality_gate_fn=quality_gate_fn,
        ))
        return self.outcome


_recorder_conformance: DispatchCoreFn = RecordingDispatchCore()
