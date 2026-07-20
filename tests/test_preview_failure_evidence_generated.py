"""Generated preview-failure HAVE convergence contracts — issue #764.

The preview worker has many ways to arrive at ``measurement_failed``.  The
failure origin must not decide whether the terminal Recents audit receives a
canonical, pre-attempt HAVE snapshot: that is one terminal-boundary contract.

Fault-injection qualification for this harness is deliberately small and
structural: deleting or reason-gating preparation loses the current FK;
moving preparation after the audit flips the pre-attempt bit; skipping force,
automation, or YouTube jobs is caught by the job-type cross-product; allowing
prepare/enrich exceptions to escape loses the terminal audit; and fabricating
evidence for an absent/unreadable exact release fails the negative oracle.
"""

from __future__ import annotations

import configparser
import json
import os
import tempfile
import unittest
from collections.abc import Mapping
from dataclasses import dataclass, replace
from typing import Any, Literal, cast
from unittest.mock import patch

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)

from hypothesis import example, given
from hypothesis import strategies as st
import msgspec

from lib.beets_db import AlbumInfo
from lib.config import CratediggerConfig
from lib.import_preview import (
    ImportPreviewResult,
    enrich_incomplete_current_evidence_for_request,
    prepare_current_evidence_for_failure,
)
from lib.import_queue import (
    IMPORT_JOB_AUTOMATION,
    IMPORT_JOB_FORCE,
    IMPORT_JOB_YOUTUBE,
    automation_import_payload,
    force_import_payload,
    youtube_import_payload,
)
from lib.quality import (
    AlbumQualityEvidence,
    AudioQualityMeasurement,
    MeasurementFailure,
    MeasurementFailureReason,
    SpectralAnalysisDetail,
    V0ProbeEvidence,
)
from lib.quality_evidence import snapshot_audio_files
from scripts import import_preview_worker
from tests.fakes import FakeBeetsDB, FakePipelineDB
from tests.helpers import make_album_quality_evidence, make_request_row


FailureMode = Literal[
    "returned",
    "raised",
    "evidence_ready_without_fk",
    "unexpected_verdict",
]
JobType = Literal["automation_import", "force_import", "youtube_import"]
OwnerShape = Literal["exact", "missing_mbid", "orphan"]
LibraryShape = Literal["installed", "absent", "unreadable"]
CurrentShape = Literal["none", "unlinked", "linked_partial", "linked_complete"]
HookFault = Literal["none", "prepare", "enrich"]


@dataclass(frozen=True)
class FailureStage:
    """One producer stage that converges on the worker's terminal sink."""

    name: str
    reason: MeasurementFailureReason
    decision: str
    mode: FailureMode = "returned"


# This is producer taxonomy, not a second production router.  Repeated reason
# tags are intentional: the property proves every origin reaches the same
# lifecycle owner instead of accidentally depending on a coarse reason string.
FAILURE_STAGES: tuple[FailureStage, ...] = (
    FailureStage("request_lookup", "request_not_found", "request_not_found"),
    FailureStage("release_identity", "missing_release_id", "missing_release_id"),
    FailureStage("source_path", "source_vanished", "path_missing"),
    FailureStage("source_snapshot", "snapshot_stale", "evidence_snapshot_failed"),
    FailureStage("materialization", "materialization_error", "materialization_failed"),
    FailureStage("candidate_measurement", "measurement_crashed", "measurement_crashed"),
    FailureStage("lossless_spectral", "measurement_crashed", "spectral_analysis_failed"),
    FailureStage("measurement_snapshot_guard", "snapshot_stale", "source_changed_during_preview"),
    FailureStage("measurement_evidence_write", "evidence_persist_failed", "evidence_persist_failed"),
    FailureStage("measurement_evidence_status", "evidence_persist_failed", "evidence_incomplete"),
    FailureStage("harness_start", "measurement_crashed", "harness_crashed"),
    FailureStage("harness_protocol", "measurement_crashed", "no_json_result"),
    FailureStage("source_conversion", "measurement_crashed", "conversion_failed"),
    FailureStage("target_conversion", "measurement_crashed", "target_conversion_failed"),
    FailureStage("harness_runtime", "measurement_crashed", "crash"),
    FailureStage("source_measurement", "measurement_crashed", "missing_source_measurement"),
    FailureStage("harness_snapshot_guard", "snapshot_stale", "source_changed_during_preview"),
    FailureStage("candidate_evidence_write", "evidence_persist_failed", "evidence_persist_failed"),
    FailureStage("candidate_evidence_status", "evidence_persist_failed", "evidence_stale"),
    FailureStage("worker_envelope", "measurement_crashed", "measurement_crashed", "raised"),
    FailureStage(
        "worker_evidence_belt",
        "evidence_persist_failed",
        "evidence_persist_failed",
        "evidence_ready_without_fk",
    ),
    FailureStage(
        "worker_unknown_verdict",
        "measurement_crashed",
        "unexpected_verdict",
        "unexpected_verdict",
    ),
)
_SOURCE_VANISHED_STAGE = next(
    stage for stage in FAILURE_STAGES if stage.name == "source_path"
)


@dataclass(frozen=True)
class PreviewFailureWorld:
    stage: FailureStage
    job_type: JobType
    owner: OwnerShape
    library: LibraryShape
    current: CurrentShape
    storage_format: str
    minimum: int
    average: int
    median: int
    hook_fault: HookFault


@st.composite
def preview_failure_worlds(draw: st.DrawFn) -> PreviewFailureWorld:
    minimum = draw(st.integers(min_value=1, max_value=2_000))
    return PreviewFailureWorld(
        stage=draw(st.sampled_from(FAILURE_STAGES)),
        job_type=draw(st.sampled_from((
            IMPORT_JOB_AUTOMATION,
            IMPORT_JOB_FORCE,
            IMPORT_JOB_YOUTUBE,
        ))),
        owner=draw(st.sampled_from(("exact", "missing_mbid", "orphan"))),
        library=draw(st.sampled_from(("installed", "absent", "unreadable"))),
        current=draw(st.sampled_from((
            "none", "unlinked", "linked_partial", "linked_complete",
        ))),
        storage_format=draw(st.sampled_from(("MP3", "Opus", "FLAC"))),
        minimum=minimum,
        average=minimum + draw(st.integers(min_value=0, max_value=100)),
        median=minimum + draw(st.integers(min_value=0, max_value=100)),
        hook_fault=draw(st.sampled_from(("none", "prepare", "enrich"))),
    )


@dataclass(frozen=True)
class PreviewFailureObservation:
    request_owned: bool
    should_prepare: bool
    should_enrich: bool
    before_current_id: int | None
    after_current_id: int | None
    job_status: str
    preview_status: str | None
    preview_result: dict[str, Any] | None
    audit: Mapping[str, object] | None
    current_evidence: AlbumQualityEvidence | None
    expected_mbid: str
    expected_path: str
    expected_format: str
    expected_minimum: int
    expected_average: int
    expected_median: int


def assert_preview_failure_have_contract(
    observation: PreviewFailureObservation,
) -> None:
    """Every eligible preview failure exposes one pre-attempt HAVE snapshot."""

    if observation.job_status != "failed" \
            or observation.preview_status != "measurement_failed":
        raise AssertionError("preview failure did not terminate its import job")

    if not observation.request_owned:
        if observation.audit is not None:
            raise AssertionError("orphan preview failure fabricated an audit owner")
        return

    audit = observation.audit
    if audit is None:
        raise AssertionError("request-owned preview failure lost its terminal audit")
    raw_validation = audit.get("validation_result")
    if not isinstance(raw_validation, str):
        raise AssertionError("terminal audit lost the typed failure payload")
    detail = json.loads(raw_validation).get("detail")
    if not isinstance(detail, str) or not detail:
        raise AssertionError("terminal audit lost the diagnostic detail")
    if audit.get("beets_detail") != detail or audit.get("error_message") != detail:
        raise AssertionError("terminal failure sinks disagree on the diagnostic")
    preview_result = observation.preview_result
    preview_failure = (
        preview_result.get("failure")
        if isinstance(preview_result, dict)
        else None
    )
    if (
        not isinstance(preview_result, dict)
        or preview_result.get("detail") != detail
        or not isinstance(preview_failure, dict)
        or preview_failure.get("detail") != detail
    ):
        raise AssertionError("job preview result disagrees on the diagnostic")

    if not observation.should_prepare:
        if observation.before_current_id is None \
                and observation.after_current_id is not None:
            raise AssertionError("ineligible failure fabricated current evidence")
        if observation.before_current_id is not None \
                and observation.after_current_id != observation.before_current_id:
            raise AssertionError("ineligible failure rewrote existing current evidence")
        return

    evidence = observation.current_evidence
    if evidence is None or observation.after_current_id is None:
        raise AssertionError("installed exact release remained without linked HAVE")
    if evidence.id != observation.after_current_id:
        raise AssertionError("request FK and loaded current evidence disagree")
    if evidence.mb_release_id != observation.expected_mbid:
        raise AssertionError("current evidence belongs to a different release")
    if os.path.realpath(evidence.source_path) != os.path.realpath(
        observation.expected_path
    ):
        raise AssertionError("current evidence describes the candidate, not HAVE")
    measurement = evidence.measurement
    actual_core = (
        measurement.format,
        measurement.min_bitrate_kbps,
        measurement.avg_bitrate_kbps,
        measurement.median_bitrate_kbps,
    )
    expected_core = (
        observation.expected_format,
        observation.expected_minimum,
        observation.expected_average,
        observation.expected_median,
    )
    if actual_core != expected_core:
        raise AssertionError(
            f"current evidence core is partial: {actual_core!r} != {expected_core!r}"
        )
    if not evidence.files:
        raise AssertionError("current evidence lost the installed file snapshot")
    if observation.should_enrich and (
        measurement.spectral_grade is None
        or (
            evidence.v0_metric is None
            and not evidence.on_disk_v0_research_attempted
        )
    ):
        raise AssertionError("post-terminal HAVE enrichment remained partial")
    if audit.get("_current_evidence_id") != observation.after_current_id:
        raise AssertionError("terminal audit did not project linked current evidence")
    if audit.get("_current_evidence_is_pre_attempt") is not True:
        raise AssertionError("current evidence was linked after the terminal audit")


class _UnreadableFakeBeetsDB(FakeBeetsDB):
    def get_album_info(self, mb_release_id: str, _cfg: Any = None) -> Any:
        self.get_album_info_calls.append(mb_release_id)
        raise OSError("generated Beets library read failure")


def _config(beets_directory: str) -> CratediggerConfig:
    ini = configparser.ConfigParser()
    ini["Beets"] = {"directory": beets_directory}
    ini["Beets Validation"] = {
        "harness_path": "/fake/harness/run_beets_harness.sh",
        "audio_check": "off",
    }
    ini["Pipeline DB"] = {"enabled": "true"}
    return CratediggerConfig.from_ini(ini)


def _job_payload(
    job_type: JobType,
    *,
    request_id: int,
    source_path: str,
) -> dict[str, Any]:
    if job_type == IMPORT_JOB_FORCE:
        return force_import_payload(
            download_log_id=764,
            failed_path=source_path,
            source_username="generated-peer",
        )
    if job_type == IMPORT_JOB_YOUTUBE:
        return youtube_import_payload(
            staged_path=source_path,
            request_id=request_id,
            browse_id="MPREb_generated_764",
        )
    return automation_import_payload()


def _preview_fn_for_world(
    world: PreviewFailureWorld,
    *,
    request_id: int | None,
    source_path: str,
):
    detail = f"{world.stage.name}: generated diagnostic"

    def preview(_db: Any, _job: Any) -> ImportPreviewResult:
        if world.stage.mode == "raised":
            raise RuntimeError(detail)
        if world.stage.mode == "evidence_ready_without_fk":
            return ImportPreviewResult(
                mode="path",
                verdict="evidence_ready",
                decision="evidence_ready",
                detail=detail,
                request_id=request_id,
                source_path=source_path,
            )
        if world.stage.mode == "unexpected_verdict":
            return ImportPreviewResult(
                mode="path",
                verdict="uncertain",
                decision="uncertain",
                detail=detail,
                request_id=request_id,
                source_path=source_path,
            )
        failure = MeasurementFailure(
            reason=world.stage.reason,
            detail=detail,
            source_path=source_path,
        )
        return ImportPreviewResult(
            mode="path",
            verdict="measurement_failed",
            decision=world.stage.decision,
            reason=world.stage.reason,
            detail=detail,
            request_id=request_id,
            source_path=source_path,
            failure=failure,
        )

    return preview


def _run_world(world: PreviewFailureWorld) -> PreviewFailureObservation:
    request_id = 764
    mbid = "generated-preview-failure-mbid"
    extension = {"MP3": "mp3", "Opus": "opus", "FLAC": "flac"}[
        world.storage_format
    ]

    with tempfile.TemporaryDirectory(
        prefix="cratedigger-preview-failure-gen-"
    ) as root:
        candidate_path = os.path.join(root, "candidate")
        installed_path = os.path.join(root, "installed")
        os.makedirs(candidate_path)
        os.makedirs(installed_path)
        with open(os.path.join(candidate_path, f"01.{extension}"), "wb") as handle:
            handle.write(b"generated candidate bytes")
        with open(os.path.join(installed_path, f"01.{extension}"), "wb") as handle:
            handle.write(b"generated installed bytes")

        db = FakePipelineDB()
        request_owned = world.owner != "orphan"
        request_mbid = mbid if world.owner == "exact" else ""
        if request_owned:
            db.seed_request(make_request_row(
                id=request_id,
                mb_release_id=request_mbid,
                status=(
                    "downloading"
                    if world.job_type == IMPORT_JOB_AUTOMATION
                    else "unsearchable"
                ),
            ))

        fake_beets: FakeBeetsDB
        if world.library == "unreadable":
            fake_beets = _UnreadableFakeBeetsDB()
        else:
            fake_beets = FakeBeetsDB()
        if world.library == "installed":
            fake_beets.set_album_info(mbid, AlbumInfo(
                album_id=1,
                track_count=1,
                min_bitrate_kbps=world.minimum,
                avg_bitrate_kbps=world.average,
                median_bitrate_kbps=world.median,
                is_cbr=False,
                album_path=installed_path,
                format=world.storage_format,
            ))

        before_current_id: int | None = None
        if world.current != "none" and request_owned:
            complete = world.current in ("unlinked", "linked_complete")
            evidence = make_album_quality_evidence(
                mb_release_id=mbid,
                source_path=installed_path,
                files=snapshot_audio_files(installed_path),
                measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=world.minimum,
                    avg_bitrate_kbps=world.average,
                    median_bitrate_kbps=world.median,
                    format=world.storage_format,
                    is_cbr=False,
                    spectral_grade="suspect" if complete else None,
                    spectral_bitrate_kbps=96 if complete else None,
                ),
                on_disk_v0_research_attempted=complete,
                codec=extension,
                container=extension,
                storage_format=world.storage_format,
            )
            db.upsert_album_quality_evidence(evidence)
            stored = db.find_album_quality_evidence(
                mb_release_id=mbid,
                snapshot_fingerprint=evidence.snapshot_fingerprint,
            )
            assert stored is not None and stored.id is not None
            if world.current.startswith("linked"):
                db.set_request_current_evidence(request_id, stored.id)
                before_current_id = stored.id

        job_request_id = request_id if request_owned else None
        job = db.enqueue_import_job(
            world.job_type,
            request_id=job_request_id,
            dedupe_key=f"generated:{world.job_type}:{world.stage.name}",
            payload=_job_payload(
                world.job_type,
                request_id=request_id,
                source_path=candidate_path,
            ),
        )
        claimed = db.claim_next_import_preview_job(worker_id="generated-preview")
        assert claimed is not None and claimed.id == job.id

        def prepare_have(db_arg: Any, **kwargs: Any) -> str:
            if world.hook_fault == "prepare":
                raise RuntimeError("generated prepare failure")
            return prepare_current_evidence_for_failure(db_arg, **kwargs)

        def enrich_have(db_arg: Any, **kwargs: Any) -> str:
            if world.hook_fault == "enrich":
                raise RuntimeError("generated enrich failure")
            return enrich_incomplete_current_evidence_for_request(
                db_arg,
                **kwargs,
                spectral_analyzer=lambda _path: SpectralAnalysisDetail(
                    attempted=True,
                    grade="suspect",
                    bitrate_kbps=96,
                ),
                probe_fn=lambda _path: V0ProbeEvidence(
                    kind="on_disk_research_v0",
                    min_bitrate_kbps=world.minimum,
                    avg_bitrate_kbps=world.average,
                    median_bitrate_kbps=world.median,
                ),
            )

        with patch(
            "scripts.import_preview_worker.read_runtime_config",
            return_value=_config(installed_path),
        ), patch(
            "lib.beets_db.BeetsDB",
            lambda **_kwargs: fake_beets,
        ), patch(
            "scripts.import_preview_worker.logger.exception",
        ), patch(
            "scripts.import_preview_worker.logger.warning",
        ), patch(
            "lib.import_preview.logger.warning",
        ):
            updated = import_preview_worker.process_claimed_preview_job(
                cast(Any, db),
                claimed,
                preview_fn=_preview_fn_for_world(
                    world,
                    request_id=job_request_id,
                    source_path=candidate_path,
                ),
                prepare_failure_have_fn=prepare_have,
                enrich_failure_have_fn=enrich_have,
            )

        assert updated is not None
        after_current_id = (
            db.get_request_current_evidence_id(request_id)
            if request_owned
            else None
        )
        current_evidence = db.load_album_quality_evidence_by_id(after_current_id)
        audit = next(
            (
                row for row in db.get_log(limit=100)
                if row.get("outcome") == "measurement_failed"
            ),
            None,
        )
        should_prepare = (
            world.owner == "exact"
            and world.library == "installed"
            and world.hook_fault != "prepare"
        )
        return PreviewFailureObservation(
            request_owned=request_owned,
            should_prepare=should_prepare,
            should_enrich=(should_prepare and world.hook_fault != "enrich"),
            before_current_id=before_current_id,
            after_current_id=after_current_id,
            job_status=updated.status,
            preview_status=updated.preview_status,
            preview_result=updated.preview_result,
            audit=audit,
            current_evidence=current_evidence,
            expected_mbid=mbid,
            expected_path=installed_path,
            expected_format=world.storage_format,
            expected_minimum=world.minimum,
            expected_average=world.average,
            expected_median=world.median,
        )


_EARLY_SOURCE_VANISHED_WORLD = PreviewFailureWorld(
    stage=_SOURCE_VANISHED_STAGE,
    job_type=IMPORT_JOB_AUTOMATION,
    owner="exact",
    library="installed",
    current="none",
    storage_format="Opus",
    minimum=90,
    average=97,
    median=95,
    hook_fault="none",
)


class TestPreviewFailureEvidenceGenerated(unittest.TestCase):
    def test_early_source_vanished_prepares_badlands_shaped_have(self) -> None:
        assert_preview_failure_have_contract(
            _run_world(_EARLY_SOURCE_VANISHED_WORLD)
        )

    def test_every_declared_failure_stage_reaches_the_same_boundary(self) -> None:
        for stage in FAILURE_STAGES:
            with self.subTest(stage=stage.name):
                assert_preview_failure_have_contract(_run_world(replace(
                    _EARLY_SOURCE_VANISHED_WORLD,
                    stage=stage,
                )))

    def test_prepare_and_enrich_exceptions_are_fail_soft(self) -> None:
        for hook_fault in ("prepare", "enrich"):
            with self.subTest(hook_fault=hook_fault):
                assert_preview_failure_have_contract(_run_world(replace(
                    _EARLY_SOURCE_VANISHED_WORLD,
                    hook_fault=cast(HookFault, hook_fault),
                )))

    @given(world=preview_failure_worlds())
    @example(world=_EARLY_SOURCE_VANISHED_WORLD)
    def test_every_failure_stage_converges_at_the_terminal_boundary(
        self,
        world: PreviewFailureWorld,
    ) -> None:
        assert_preview_failure_have_contract(_run_world(world))


class TestPreviewFailureEvidenceCheckerKnownBad(unittest.TestCase):
    def _observation(self, **overrides: Any) -> PreviewFailureObservation:
        evidence = msgspec.structs.replace(make_album_quality_evidence(
            mb_release_id="generated-preview-failure-mbid",
            source_path="/library/installed",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=90,
                avg_bitrate_kbps=97,
                median_bitrate_kbps=95,
                format="Opus",
            ),
        ), id=7)
        base = {
            "request_owned": True,
            "should_prepare": True,
            "should_enrich": False,
            "before_current_id": None,
            "after_current_id": 7,
            "job_status": "failed",
            "preview_status": "measurement_failed",
            "preview_result": {
                "detail": "decoder failed",
                "failure": {"detail": "decoder failed"},
            },
            "audit": {
                "validation_result": json.dumps({"detail": "decoder failed"}),
                "beets_detail": "decoder failed",
                "error_message": "decoder failed",
                "_current_evidence_id": 7,
                "_current_evidence_is_pre_attempt": True,
            },
            "current_evidence": evidence,
            "expected_mbid": "generated-preview-failure-mbid",
            "expected_path": "/library/installed",
            "expected_format": "Opus",
            "expected_minimum": 90,
            "expected_average": 97,
            "expected_median": 95,
        }
        base.update(overrides)
        return PreviewFailureObservation(**base)

    def test_trips_when_installed_release_has_no_current_evidence(self) -> None:
        with self.assertRaisesRegex(AssertionError, "without linked HAVE"):
            assert_preview_failure_have_contract(self._observation(
                after_current_id=None,
                current_evidence=None,
                audit={
                    "validation_result": json.dumps({"detail": "decoder failed"}),
                    "beets_detail": "decoder failed",
                    "error_message": "decoder failed",
                    "_current_evidence_id": None,
                    "_current_evidence_is_pre_attempt": None,
                },
            ))

    def test_trips_when_have_is_linked_after_the_audit(self) -> None:
        audit = cast(dict[str, object], self._observation().audit)
        with self.assertRaisesRegex(AssertionError, "after the terminal audit"):
            assert_preview_failure_have_contract(self._observation(
                audit={**audit, "_current_evidence_is_pre_attempt": False},
            ))

    def test_trips_when_diagnostic_sink_is_empty(self) -> None:
        audit = cast(dict[str, object], self._observation().audit)
        with self.assertRaisesRegex(AssertionError, "sinks disagree"):
            assert_preview_failure_have_contract(self._observation(
                audit={**audit, "error_message": None},
            ))

    def test_trips_when_successful_enrichment_remains_partial(self) -> None:
        with self.assertRaisesRegex(AssertionError, "enrichment remained partial"):
            assert_preview_failure_have_contract(self._observation(
                should_enrich=True,
            ))

    def test_trips_when_absent_library_fabricates_evidence(self) -> None:
        with self.assertRaisesRegex(AssertionError, "fabricated current evidence"):
            assert_preview_failure_have_contract(self._observation(
                should_prepare=False,
                after_current_id=7,
            ))

    def test_trips_when_job_is_not_terminal(self) -> None:
        with self.assertRaisesRegex(AssertionError, "did not terminate"):
            assert_preview_failure_have_contract(self._observation(
                job_status="queued",
            ))


if __name__ == "__main__":
    unittest.main()
