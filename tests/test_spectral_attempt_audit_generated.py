"""Generated invariant for independent two-sided spectral attempt audit."""

import configparser
import logging
import os
import subprocess as sp
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Literal
from unittest.mock import patch

import msgspec
from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401  (loads active profile)
from lib.spectral_check import SPECTRAL_MEASUREMENT_VERSION
from tests.beets_world import BeetsWorld
from tests.dispatch_helpers import (
    claim_next_import_job,
    claim_next_import_preview_job,
)
from tests.test_pipeline_db import make_db, requires_postgres


@contextmanager
def _silence_logs():
    previous_level = logging.root.manager.disable
    logging.disable(logging.CRITICAL)
    try:
        yield
    finally:
        logging.disable(previous_level)


def _policy_snapshot(result):
    return (result.decision, result.source_measurement, result.current_measurement)


def _policy_snapshot_unchanged(before, after) -> bool:
    return _policy_snapshot(after) == before


def _have_preserves_persisted_source(
    audit,
    *,
    expected_grade: str,
    expected_bitrate: int | None,
    analyzer_calls: list[object],
) -> bool:
    existing = audit.existing
    return (
        existing is not None
        and existing.grade == expected_grade
        and existing.bitrate_kbps == expected_bitrate
        and analyzer_calls == ["candidate"]
    )


def _have_scan_boundary_holds(
    analyzer_calls: list[str],
    *,
    preserve_existing_source: bool,
    candidate_reused: bool,
    reuse_have: bool,
) -> bool:
    expected = [] if candidate_reused else ["candidate"]
    if not preserve_existing_source and not reuse_have:
        expected.append("existing")
    return analyzer_calls == expected


def _have_reuse_contract_holds(
    *,
    reuse_have: bool,
    preserve_source: bool,
    have_complete: bool,
    snapshot_changed: bool,
    persisted_grade: str | None,
    persisted_generation: int | None,
) -> bool:
    from lib.spectral_check import SPECTRAL_MEASUREMENT_VERSION

    expected = (
        have_complete
        and not snapshot_changed
        and persisted_grade in {
            "genuine",
            "marginal",
            "suspect",
            "likely_transcode",
        }
        and (
            preserve_source
            or persisted_generation == SPECTRAL_MEASUREMENT_VERSION
        )
    )
    return reuse_have is expected


def assert_iron_and_wine_outer_policy(
    *,
    analyzer_roles: list[str],
    persisted_grade: str | None,
    persisted_generation: int | None,
    dispatch_code: str | None,
    request_status: str,
    outcomes: list[str],
    canonical_source_removed: bool,
    cleanup_receipt_recorded: bool,
    job_terminal: bool,
    owner_cleared: bool,
) -> None:
    """Name the complete #1007 automation witness at the importer boundary."""

    if analyzer_roles != ["candidate"]:
        raise AssertionError("preserved source triggered a derivative HAVE scan")
    if (persisted_grade, persisted_generation) != ("likely_transcode", None):
        raise AssertionError("preserved source provenance was rewritten")
    if dispatch_code != "quality_pipeline_rejected":
        raise AssertionError("preserved source did not reach policy dispatch")
    if request_status != "wanted":
        raise AssertionError("quality rejection did not return the request to wanted")
    if "have_analysis_error" in outcomes:
        raise AssertionError("policy dispatch became an analyser failure")
    if not canonical_source_removed:
        raise AssertionError("automation source was not removed by owned cleanup")
    if not cleanup_receipt_recorded:
        raise AssertionError("automation terminal result lacks cleanup receipt")
    if not job_terminal:
        raise AssertionError("automation import job was not terminal")
    if not owner_cleared:
        raise AssertionError("automation owner was not cleared with terminal state")


def _run_have_boundary_through_both_adapters(
    *,
    converted_from: str | None,
    lossless_v0_lineage: bool,
    persisted_grade: str | None,
    persisted_bitrate: int | None,
    scanned_grade: str,
    scanned_bitrate: int | None,
    persisted_generation: int | None = None,
    have_complete: bool = True,
    snapshot_changed: bool = False,
    candidate_grade: str = "genuine",
    installed: tuple[str, str] = ("mp3", "MP3"),
):
    """Drive normal measurement and reused front-gate through one boundary."""
    from lib.beets_db import AlbumInfo
    from lib.config import CratediggerConfig
    from lib.current_library_evidence import (
        authorize_current_evidence_for_preview,
        current_spectral_evidence_reusable,
        preserve_existing_source_spectral,
    )
    from lib.import_queue import (
        IMPORT_JOB_FORCE,
        force_import_dedupe_key,
        force_import_payload,
    )
    from lib.measurement import (
        ExistingSpectralAuditLookup,
        LocalFileInspection,
        measure_preimport_state,
    )
    from lib.quality import (
        EVIDENCE_SUBJECT_SOURCE,
        AlbumQualityV0Metric,
        AudioQualityMeasurement,
        ImportResult,
        SpectralAnalysisDetail,
    )
    from lib.quality_evidence import snapshot_audio_files
    from lib.spectral_check import SPECTRAL_MEASUREMENT_VERSION
    from scripts.import_preview_worker import process_claimed_preview_job
    from tests.evidence_helpers import make_album_quality_evidence
    from tests.fakes import FakeBeetsDB, FakePipelineDB
    from tests.helpers import make_request_row

    request_id = 42
    mbid = "mbid-42"
    carries_lossless_lineage = (
        (converted_from or "").lower() in {"flac", "alac", "wav"}
        or lossless_v0_lineage
    )
    installed_extension, installed_format = installed
    current_measurement = AudioQualityMeasurement(
        min_bitrate_kbps=320,
        avg_bitrate_kbps=320,
        median_bitrate_kbps=320,
        format=installed_format,
        spectral_grade=persisted_grade if have_complete else None,
        spectral_bitrate_kbps=persisted_bitrate if have_complete else None,
        was_converted_from=converted_from,
        spectral_subject=(
            ("source" if carries_lossless_lineage else "installed")
            if have_complete
            else None
        ),
        spectral_provenance=(
            ("carried" if carries_lossless_lineage else "measured")
            if have_complete
            else None
        ),
        spectral_measurement_version=(
            persisted_generation if have_complete else None
        ),
    )
    current_v0_metric = (
        AlbumQualityV0Metric(
            min_bitrate_kbps=200,
            avg_bitrate_kbps=228,
            median_bitrate_kbps=225,
            subject=EVIDENCE_SUBJECT_SOURCE,
            provenance="measured",
        )
        if lossless_v0_lineage
        else None
    )

    with tempfile.TemporaryDirectory() as root, \
         tempfile.TemporaryDirectory() as existing:
        staging_dir = os.path.join(root, "Incoming")
        candidate = os.path.join(
            staging_dir,
            "failed_imports",
            "candidate",
        )
        os.makedirs(candidate)
        slskd_dir = os.path.join(root, "slskd")
        os.makedirs(slskd_dir)
        processing_dir = os.path.join(root, "processing")
        os.makedirs(processing_dir, mode=0o700)
        os.makedirs(os.path.join(processing_dir, "albums"), mode=0o700)
        os.makedirs(os.path.join(processing_dir, "preview"), mode=0o700)
        cfg = CratediggerConfig(
            audio_check_mode="off",
            beets_staging_dir=staging_dir,
            slskd_download_dir=slskd_dir,
            processing_dir=processing_dir,
        )
        Path(candidate, "01.flac").write_bytes(b"candidate")
        Path(existing, f"01.{installed_extension}").write_bytes(b"existing")
        current_evidence = make_album_quality_evidence(
            preserve_spectral_measurement_version=True,
            mb_release_id=mbid,
            source_path=existing,
            files=snapshot_audio_files(existing),
            measurement=current_measurement,
            v0_metric=current_v0_metric,
            codec=installed_extension,
            container=installed_extension,
            storage_format=installed_format,
        )
        if snapshot_changed:
            Path(existing, f"01.{installed_extension}").write_bytes(
                b"changed-existing"
            )
        fake_beets = FakeBeetsDB()
        fake_beets.set_album_info(mbid, AlbumInfo(
            album_id=1,
            track_count=1,
            min_bitrate_kbps=320,
            avg_bitrate_kbps=320,
            median_bitrate_kbps=320,
            is_cbr=True,
            album_path=existing,
            format=installed_format,
        ))
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=request_id, mb_release_id=mbid))
        db.upsert_album_quality_evidence(current_evidence)
        stored_current = db.find_album_quality_evidence(
            mb_release_id=mbid,
            snapshot_fingerprint=current_evidence.snapshot_fingerprint,
        )
        assert stored_current is not None and stored_current.id is not None
        db.set_request_current_evidence(request_id, stored_current.id)
        with _silence_logs(), patch(
            "lib.beets_db.BeetsDB",
            lambda *_args, **_kwargs: fake_beets,
        ):
            authorized = authorize_current_evidence_for_preview(
                db,
                request_id=request_id,
                mb_release_id=mbid,
                quality_ranks=cfg.quality_ranks,
                beets_library_root=existing,
                preloaded_evidence=stored_current,
            )
        assert authorized.status == "ready"
        assert authorized.evidence is not None
        authorized_current = authorized.evidence
        preserve_source = preserve_existing_source_spectral(
            authorized_current,
        )
        authorized_measurement = authorized_current.measurement
        persisted = SpectralAnalysisDetail(
            attempted=(
                authorized_measurement.spectral_grade is not None
                or authorized_measurement.spectral_bitrate_kbps is not None
            ),
            grade=authorized_measurement.spectral_grade,
            bitrate_kbps=authorized_measurement.spectral_bitrate_kbps,
            cliff_hz=authorized_measurement.cliff_hz,
            codec_family=authorized_measurement.codec_family,
            ultrasonic_deficit_db=(
                authorized_measurement.ultrasonic_deficit_db
            ),
            spectral_measurement_version=(
                authorized_measurement.spectral_measurement_version
            ),
        )
        reuse_have = current_spectral_evidence_reusable(
            authorized_current,
        )

        normal_calls: list[str] = []
        reused_calls: list[str] = []

        def analyzer_for(calls: list[str]):
            def analyze(path: str) -> SpectralAnalysisDetail:
                role = "existing" if path == existing else "candidate"
                calls.append(role)
                return SpectralAnalysisDetail(
                    attempted=True,
                    grade=(
                        scanned_grade if role == "existing" else candidate_grade
                    ),
                    bitrate_kbps=(
                        scanned_bitrate if role == "existing" else None
                    ),
                    spectral_measurement_version=(
                        SPECTRAL_MEASUREMENT_VERSION
                    ),
                )
            return analyze

        def resolve_existing(
            requested_mbid: str,
        ) -> ExistingSpectralAuditLookup:
            assert requested_mbid == mbid
            return ExistingSpectralAuditLookup(
                path=existing,
                min_bitrate_kbps=320,
            )

        with _silence_logs():
            measured = measure_preimport_state(
                path=candidate,
                mb_release_id=mbid,
                label="Gespenst - The Saint",
                download_filetype="flac",
                download_min_bitrate_bps=219_000,
                download_is_vbr=False,
                cfg=cfg,
                existing_spectral_evidence=persisted,
                reuse_existing_spectral_evidence=reuse_have,
                preserve_existing_source_spectral=preserve_source,
                precomputed_inspection=LocalFileInspection(
                    filetype="flac",
                    min_bitrate_bps=219_000,
                    is_vbr=False,
                ),
                spectral_detail_analyzer=analyzer_for(normal_calls),
                existing_spectral_resolver=resolve_existing,
            )

        download_log_id = db.log_download(
            request_id,
            outcome="rejected",
            validation_result={"failed_path": candidate},
        )
        job = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=request_id,
            dedupe_key=force_import_dedupe_key(download_log_id),
            payload=force_import_payload(
                download_log_id=download_log_id,
                failed_path=candidate,
                source_username="generated",
            ),
        )
        from scripts import import_preview_worker

        action_path = import_preview_worker._prepare_force_action_path(
            db,
            job,
            cfg,
            raw_path=candidate,
        )
        candidate_evidence = make_album_quality_evidence(
            preserve_spectral_measurement_version=True,
            mb_release_id=mbid,
            source_path=action_path,
            files=snapshot_audio_files(action_path),
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=472,
                avg_bitrate_kbps=506,
                median_bitrate_kbps=500,
                format="FLAC",
                spectral_grade=candidate_grade,
                spectral_bitrate_kbps=96,
                spectral_subject="source",
                spectral_provenance="measured",
                spectral_measurement_version=SPECTRAL_MEASUREMENT_VERSION,
            ),
            v0_metric=AlbumQualityV0Metric(
                min_bitrate_kbps=165,
                avg_bitrate_kbps=171,
                median_bitrate_kbps=168,
                subject=EVIDENCE_SUBJECT_SOURCE,
                provenance="measured",
            ),
            codec="flac",
            container="flac",
            storage_format="FLAC",
        )
        db.upsert_album_quality_evidence(candidate_evidence)
        stored_candidate = db.find_album_quality_evidence(
            mb_release_id=mbid,
            snapshot_fingerprint=candidate_evidence.snapshot_fingerprint,
        )
        assert stored_candidate is not None and stored_candidate.id is not None
        db.set_import_job_candidate_evidence(job.id, stored_candidate.id)
        claimed = claim_next_import_preview_job(db, worker_id="generated")
        assert claimed is not None and claimed.id == job.id
        with _silence_logs(), patch(
            "lib.beets_db.BeetsDB",
            lambda *_args, **_kwargs: fake_beets,
        ):
            updated = process_claimed_preview_job(
                db,
                claimed,
                spectral_detail_analyzer=analyzer_for(reused_calls),
                existing_spectral_resolver=resolve_existing,
                runtime_config=cfg,
            )
        assert updated is not None and updated.preview_result is not None
        reused = ImportResult.from_dict(
            updated.preview_result["import_result"]
        ).spectral
    return (
        preserve_source,
        reuse_have,
        normal_calls,
        reused_calls,
        measured.spectral_audit,
        reused,
    )


PreviewJobMode = Literal["automation", "force"]


def assert_candidate_snapshot_reuse(
    *,
    snapshot_changed: bool,
    candidate_generation: int | None,
    has_have: bool,
    full_preview_calls: int,
    analyzer_roles: list[str],
    candidate_status: str | None,
    persisted_candidate_grade: str | None,
    persisted_have_grade: str | None,
    expected_candidate_grade: str,
) -> None:
    """Candidate work is once per snapshot; HAVE authority stays separate."""

    must_remeasure = (
        snapshot_changed
        or candidate_generation != SPECTRAL_MEASUREMENT_VERSION
    )
    if must_remeasure:
        if full_preview_calls != 1:
            raise AssertionError(
                "changed or old-generation candidate did not remeasure"
            )
        if candidate_status == "reused":
            raise AssertionError(
                "changed or old-generation candidate was marked reused"
            )
        return

    if full_preview_calls:
        raise AssertionError("matching candidate snapshot ran full preview")
    if "candidate" in analyzer_roles:
        raise AssertionError("matching candidate evidence was analyzed again")
    expected_roles: list[str] = []
    if analyzer_roles != expected_roles:
        raise AssertionError(
            "matching candidate or HAVE evidence was analyzed again"
        )
    if candidate_status != "reused":
        raise AssertionError("matching candidate snapshot lost reuse provenance")
    if persisted_candidate_grade != expected_candidate_grade:
        raise AssertionError("reused preview dropped persisted candidate spectral")
    expected_have_grade = "genuine" if has_have else None
    if persisted_have_grade != expected_have_grade:
        raise AssertionError(
            "reused preview bypassed or dropped exact-release HAVE evidence"
        )


def _run_candidate_snapshot_reuse_world(
    *,
    job_mode: PreviewJobMode,
    snapshot_changed: bool,
    candidate_generation: int | None,
    has_have: bool,
    candidate_grade: str,
    track_count: int,
) -> tuple[int, list[str], str | None, str | None, str | None]:
    from lib.beets_db import AlbumInfo
    from lib.config import CratediggerConfig
    from lib.import_execution import (
        CancellationToken,
        ExecutionLeaseSnapshot,
        ProcessIdentity,
    )
    from lib.import_preview import ImportPreviewResult
    from lib.import_queue import (
        IMPORT_JOB_FORCE,
        force_import_dedupe_key,
        force_import_payload,
    )
    from lib.measurement import ExistingSpectralAuditLookup
    from lib.quality import (
        ActiveDownloadState,
        AudioQualityMeasurement,
        ImportResult,
        SpectralAnalysisDetail,
    )
    from lib.quality_evidence import snapshot_audio_files
    from scripts import import_preview_worker
    from scripts.import_preview_worker import process_claimed_preview_job
    from tests.evidence_helpers import make_album_quality_evidence
    from tests.fakes import FakeBeetsDB, FakePipelineDB
    from tests.helpers import make_request_row

    request_id = 8883
    mbid = "generated-candidate-reuse-mbid"
    with tempfile.TemporaryDirectory() as root, \
         tempfile.TemporaryDirectory() as existing:
        staging_dir = os.path.join(root, "Incoming")
        candidate = os.path.join(
            staging_dir,
            "failed_imports",
            "candidate",
        )
        os.makedirs(candidate)
        slskd_dir = os.path.join(root, "slskd")
        os.makedirs(slskd_dir)
        processing_dir = os.path.join(root, "processing")
        os.makedirs(processing_dir, mode=0o700)
        os.makedirs(os.path.join(processing_dir, "albums"), mode=0o700)
        os.makedirs(os.path.join(processing_dir, "preview"), mode=0o700)
        ini = configparser.ConfigParser()
        ini["Beets Validation"] = {
            "harness_path": "/fake/harness/run_beets_harness.sh",
            "audio_check": "off",
            "staging_dir": staging_dir,
        }
        ini["Slskd"] = {"download_dir": slskd_dir}
        ini["Paths"] = {"processing_dir": processing_dir}
        ini["Pipeline DB"] = {"enabled": "true"}
        cfg = CratediggerConfig.from_ini(ini)
        for track in range(1, track_count + 1):
            Path(candidate, f"{track:02d}.mp3").write_bytes(
                f"candidate-{track}".encode()
            )
        Path(existing, "01.mp3").write_bytes(b"installed-have")

        db = FakePipelineDB()
        active_state = {
            "filetype": "mp3",
            "enqueued_at": "2026-07-21T00:00:00+00:00",
            "current_path": candidate,
            "files": [
                {
                    "username": "generated-peer",
                    "filename": f"Artist\\Album\\{track:02d}.mp3",
                    "file_dir": "Artist\\Album",
                    "size": track,
                }
                for track in range(1, track_count + 1)
            ],
        }
        db.seed_request(make_request_row(
            id=request_id,
            mb_release_id=mbid,
            status="downloading" if job_mode == "automation" else "wanted",
            active_download_state=(
                active_state if job_mode == "automation" else None
            ),
        ))

        current = None
        if has_have:
            current = make_album_quality_evidence(
                mb_release_id=mbid,
                source_path=existing,
                files=snapshot_audio_files(existing),
                measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=192,
                    avg_bitrate_kbps=192,
                    median_bitrate_kbps=192,
                    format="MP3",
                    is_cbr=True,
                    spectral_grade="genuine",
                    spectral_subject="installed",
                    spectral_provenance="measured",
                    spectral_measurement_version=(
                        SPECTRAL_MEASUREMENT_VERSION
                    ),
                ),
            )
            db.upsert_album_quality_evidence(current)
            current = db.find_album_quality_evidence(
                mb_release_id=mbid,
                snapshot_fingerprint=current.snapshot_fingerprint,
            )
            assert current is not None and current.id is not None
            db.set_request_current_evidence(request_id, current.id)

        download_log_id: int | None = None
        if job_mode == "force":
            download_log_id = db.log_download(
                request_id,
                outcome="rejected",
                validation_result={"failed_path": candidate},
            )
            job = db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=request_id,
                dedupe_key=force_import_dedupe_key(download_log_id),
                payload=force_import_payload(
                    download_log_id=download_log_id,
                    failed_path=candidate,
                    source_username="generated-peer",
                ),
            )
        else:
            handoff = db.handoff_automation_import(
                request_id=request_id,
                expected_enqueued_at=str(active_state["enqueued_at"]),
                canonical_path=candidate,
                message="generated candidate-reuse owner handoff",
            )
            assert handoff.committed and handoff.job is not None
            job = handoff.job

        action_path = candidate
        if job_mode == "force":
            action_path = import_preview_worker._prepare_force_action_path(
                db,
                job,
                cfg,
                raw_path=candidate,
            )
        candidate_evidence = make_album_quality_evidence(
            preserve_spectral_measurement_version=True,
            mb_release_id=mbid,
            source_path=action_path,
            files=snapshot_audio_files(action_path),
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=245,
                avg_bitrate_kbps=256,
                median_bitrate_kbps=252,
                format="MP3",
                spectral_grade=candidate_grade,
                spectral_subject="source",
                spectral_provenance="measured",
                spectral_measurement_version=candidate_generation,
            ),
        )
        db.upsert_album_quality_evidence(candidate_evidence)
        stored_candidate = db.find_album_quality_evidence(
            mb_release_id=mbid,
            snapshot_fingerprint=candidate_evidence.snapshot_fingerprint,
        )
        assert stored_candidate is not None and stored_candidate.id is not None

        preview_lease = (
            ExecutionLeaseSnapshot(
                host_boot_id="generated-candidate-reuse-boot",
                invocation_id=f"generated-candidate-reuse-preview-{job.id}",
                systemd_unit="cratedigger-import-preview.service",
                worker=ProcessIdentity(pid=764, start_ticks=1),
            )
            if job_mode == "automation"
            else None
        )
        claimed = claim_next_import_preview_job(db, worker_id="generated",
        execution_lease=preview_lease,)
        assert claimed is not None and claimed.id == job.id
        assert db.set_import_job_candidate_evidence(
            job.id,
            stored_candidate.id,
            expected_execution_lease=preview_lease,
        )

        if snapshot_changed:
            changed_path = candidate if job_mode == "force" else action_path
            Path(changed_path, f"{track_count:02d}.mp3").write_bytes(
                b"changed-candidate-snapshot"
            )

        automation_authority = (
            import_preview_worker._AutomationPreviewAuthority(
                request=db.request(request_id),
                state=ActiveDownloadState.from_raw(
                    db.request(request_id)["active_download_state"]
                ),
                canonical_path=action_path,
            )
            if job_mode == "automation"
            else None
        )
        cancellation_token = (
            CancellationToken() if job_mode == "automation" else None
        )
        full_preview_calls = 0
        analyzer_roles: list[str] = []

        def analyze(path: str):
            role = "existing" if path == existing else "candidate"
            analyzer_roles.append(role)
            return SpectralAnalysisDetail(
                attempted=True,
                grade="suspect" if role == "existing" else candidate_grade,
                bitrate_kbps=128 if role == "existing" else None,
                spectral_measurement_version=(
                    SPECTRAL_MEASUREMENT_VERSION
                ),
            )

        def full_preview(db_arg: Any, _job: Any) -> ImportPreviewResult:
            nonlocal full_preview_calls
            full_preview_calls += 1
            fresh = make_album_quality_evidence(
                mb_release_id=mbid,
                source_path=action_path,
                files=snapshot_audio_files(action_path),
                measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=245,
                    avg_bitrate_kbps=256,
                    median_bitrate_kbps=252,
                    format="MP3",
                    spectral_grade=candidate_grade,
                    spectral_subject="source",
                    spectral_provenance="measured",
                    spectral_measurement_version=(
                        SPECTRAL_MEASUREMENT_VERSION
                    ),
                ),
            )
            db_arg.upsert_album_quality_evidence(fresh)
            persisted = db_arg.find_album_quality_evidence(
                mb_release_id=mbid,
                snapshot_fingerprint=fresh.snapshot_fingerprint,
            )
            assert persisted is not None and persisted.id is not None
            db_arg.set_import_job_candidate_evidence(job.id, persisted.id)
            return ImportPreviewResult(
                mode="path",
                verdict="evidence_ready",
                decision="import",
                reason="import",
                source_path=action_path,
                action_path=(action_path if job_mode == "force" else None),
            )

        fake_beets = FakeBeetsDB()
        if has_have:
            fake_beets.set_album_info(mbid, AlbumInfo(
                album_id=1,
                track_count=1,
                min_bitrate_kbps=192,
                avg_bitrate_kbps=192,
                median_bitrate_kbps=192,
                is_cbr=True,
                album_path=existing,
                format="MP3",
            ))
        with _silence_logs(), patch(
            "lib.beets_db.BeetsDB",
            lambda *_args, **_kwargs: fake_beets,
        ):
            updated = process_claimed_preview_job(
                db,
                claimed,
                spectral_detail_analyzer=analyze,
                existing_spectral_resolver=lambda _release_id: (
                    ExistingSpectralAuditLookup(
                        path=existing if has_have else None,
                    )
                ),
                preview_fn=full_preview,
                runtime_config=cfg,
                execution_lease=preview_lease,
                automation_authority=automation_authority,
                cancellation_token=cancellation_token,
            )
        assert updated is not None
        preview_result = updated.preview_result or {}
        candidate_status = preview_result.get("candidate_status")
        persisted_grade = None
        persisted_have_grade = None
        import_result_raw = preview_result.get("import_result")
        if isinstance(import_result_raw, dict):
            import_result = ImportResult.from_dict(import_result_raw)
            if import_result.spectral.candidate is not None:
                persisted_grade = import_result.spectral.candidate.grade
            if import_result.spectral.existing is not None:
                persisted_have_grade = import_result.spectral.existing.grade
        return (
            full_preview_calls,
            analyzer_roles,
            candidate_status if isinstance(candidate_status, str) else None,
            persisted_grade,
            persisted_have_grade,
        )


def _authoritative_have_matches(detail, grade, bitrate) -> bool:
    return (
        detail.attempted == (grade is not None or bitrate is not None)
        and detail.grade == grade
        and detail.bitrate_kbps == bitrate
    )


def _stale_scalar_fallback_mutant(req):
    """Known-bad model: revives request scalars after empty current evidence."""
    from lib.measurement import spectral_detail_from_persisted_source
    return spectral_detail_from_persisted_source(
        req.get("current_spectral_grade"),
        req.get("current_spectral_bitrate"),
    )


def _persisted_attempt_has_exact_audit(
    import_result_json: str | None,
    expected_audit,
) -> bool:
    if import_result_json is None:
        return False
    from lib.quality import ImportResult
    return ImportResult.from_json(import_result_json).spectral == expected_audit


def _policy_payload(import_result_json: str | None) -> dict[str, Any]:
    from lib.quality import ImportResult, SpectralDetail

    result = (
        ImportResult.from_json(import_result_json)
        if import_result_json is not None
        else ImportResult()
    )
    payload = msgspec.to_builtins(result)
    assert isinstance(payload, dict)
    payload["spectral"] = msgspec.to_builtins(SpectralDetail())
    return payload


def _run_dispatch_finalization_world(
    *,
    mode: str,
    audit,
    new_bitrate: int,
    existing_bitrate: int,
    converted: bool,
    beets: BeetsWorld,
) -> dict[str, Any]:
    """Drive the real dispatch terminal writers with injected failure timing."""
    from lib.config import CratediggerConfig
    from lib.dispatch import dispatch_import_core
    from lib.dispatch.types import ImportOneRun
    from lib.import_execution import (
        CancellationToken,
        ExecutionLeaseSnapshot,
        ProcessIdentity,
    )
    from lib.quality import DownloadInfo, ImportResult, QualityComparisonBasis
    from tests.dispatch_helpers import (
        finalize_claimed_dispatch,
        make_dispatch_request,
        noop_quality_gate,
        patch_dispatch_externals,
        pinned_dispatch_authority,
    )
    from tests.evidence_helpers import make_album_quality_evidence
    from tests.fakes import FakePipelineDB
    from tests.helpers import make_import_result, make_request_row

    db = FakePipelineDB()
    db.seed_request(make_request_row(
        id=42,
        mb_release_id="generated-mbid",
        status="downloading",
        search_filetype_override="mp3",
        active_download_state={
            "files": [],
            "filetype": "mp3",
            "enqueued_at": "2026-07-21T00:00:00+00:00",
        },
    ))
    cfg = CratediggerConfig(
        beets_harness_path="/nix/store/fake/harness/run_beets_harness.sh",
        pipeline_db_enabled=True,
    )

    def rich_result() -> ImportResult:
        result = make_import_result(
            decision="downgrade" if mode == "rejection" else "import",
            new_min_bitrate=new_bitrate,
            prev_min_bitrate=existing_bitrate,
            was_converted=converted,
            original_filetype="flac" if converted else None,
            target_filetype="opus" if converted else None,
            imported_path="/Beets/Generated/Album",
            disambiguated=True,
            final_format="opus 128" if converted else "mp3 320",
        )
        result.postflight.beets_id = 77
        result.postflight.track_count = 9
        result.comparison_basis = QualityComparisonBasis(
            verdict="better" if mode != "rejection" else "worse",
            branch="rank",
            new_rank="mp3_v0",
            existing_rank="mp3_v2",
            new_value_kbps=new_bitrate,
            existing_value_kbps=existing_bitrate,
        )
        return result

    def run_import(*args: Any, **kwargs: Any) -> ImportOneRun:
        del args
        on_spawn = kwargs.pop("on_spawn", None)
        cancellation_token = kwargs.pop("cancellation_token", None)
        kwargs.pop("owner_session_probe", None)
        del kwargs
        if on_spawn is not None:
            on_spawn(os.getpid())
        if cancellation_token is not None:
            cancellation_token.raise_if_cancelled()
        if mode == "timeout":
            raise sp.TimeoutExpired("import_one", 300)
        if mode == "pre_result_exception":
            raise RuntimeError("before result")
        if mode == "no_json":
            return ImportOneRun(("import_one",), 1, "", "", None)
        return ImportOneRun(("import_one",), 0, "", "", rich_result())

    def quality_gate(**kwargs: Any) -> None:
        del kwargs
        if mode == "post_result_exception":
            raise RuntimeError("after result")
        noop_quality_gate()

    if mode == "manifest_rejection":
        from lib.dispatch import dispatch_import_from_db
        from lib.import_queue import IMPORT_JOB_FORCE
        from lib.quality_evidence import snapshot_audio_files

        db.set_tracks(42, [{"track_number": 1, "title": "One"}])
        with tempfile.TemporaryDirectory() as source:
            for filename in ("01.mp3", "bonus.mp3"):
                with open(os.path.join(source, filename), "wb") as handle:
                    handle.write(b"audio")
            job = db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                payload={"download_log_id": 1, "failed_path": source},
            )
            preview_result: dict[str, Any] = {}
            if audit is not None:
                builtins = msgspec.to_builtins(ImportResult(spectral=audit))
                assert isinstance(builtins, dict)
                preview_result["import_result"] = builtins
            evidence = make_album_quality_evidence(
                mb_release_id="generated-mbid",
                source_path=source,
                files=snapshot_audio_files(source),
            )
            db.upsert_album_quality_evidence(evidence)
            persisted = db.find_album_quality_evidence(
                mb_release_id=evidence.mb_release_id,
                snapshot_fingerprint=evidence.snapshot_fingerprint,
            )
            assert persisted is not None and persisted.id is not None
            db.set_import_job_candidate_evidence(job.id, persisted.id)
            db.mark_import_job_preview_importable(
                job.id,
                preview_result=preview_result,
            )
            claimed = claim_next_import_job(db, worker_id="generated-importer")
            assert claimed is not None and claimed.id == job.id
            with _silence_logs():
                outcome = dispatch_import_from_db(
                    db,
                    request_id=42,
                    failed_path=source,
                    import_job_id=job.id,
                    source_username="generated-user",
                    beets_library_db_path=str(beets.library_db),
                    beets_library_root=str(beets.library_root),
                )
                finalize_claimed_dispatch(db, claimed, outcome)
    else:
        from lib.quality_evidence import snapshot_audio_files

        with tempfile.TemporaryDirectory() as source:
            with open(os.path.join(source, "01.mp3"), "wb") as handle:
                handle.write(b"audio")
            db.request(42)["active_download_state"]["current_path"] = source
            handoff = db.handoff_automation_import(
                request_id=42,
                expected_enqueued_at="2026-07-21T00:00:00+00:00",
                canonical_path=source,
                message="generated dispatch-finalization owner handoff",
            )
            assert handoff.committed and handoff.job is not None
            job = handoff.job
            preview_lease = ExecutionLeaseSnapshot(
                host_boot_id="generated-dispatch-finalization-boot",
                invocation_id=f"generated-dispatch-preview-{job.id}",
                systemd_unit="cratedigger-import-preview.service",
                worker=ProcessIdentity(pid=9101, start_ticks=91001),
            )
            claimed_preview = claim_next_import_preview_job(db, worker_id="generated-preview",
            execution_lease=preview_lease,)
            assert claimed_preview is not None and claimed_preview.id == job.id
            evidence = make_album_quality_evidence(
                mb_release_id="generated-mbid",
                source_path=source,
                files=snapshot_audio_files(source),
            )
            db.upsert_album_quality_evidence(evidence)
            persisted = db.find_album_quality_evidence(
                mb_release_id=evidence.mb_release_id,
                snapshot_fingerprint=evidence.snapshot_fingerprint,
            )
            assert persisted is not None and persisted.id is not None
            assert db.set_import_job_candidate_evidence(
                job.id,
                persisted.id,
                expected_execution_lease=preview_lease,
            )
            assert db.mark_import_job_preview_importable(
                job.id,
                preview_result={"ready": True},
                expected_execution_lease=preview_lease,
            )
            importer_lease = ExecutionLeaseSnapshot(
                host_boot_id="generated-dispatch-finalization-boot",
                invocation_id=f"generated-dispatch-importer-{job.id}",
                systemd_unit="cratedigger-importer.service",
                worker=ProcessIdentity(pid=9102, start_ticks=91002),
            )
            claimed = claim_next_import_job(db, worker_id="generated-importer",
            execution_lease=importer_lease,)
            assert claimed is not None and claimed.id == job.id
            cancellation_token = CancellationToken()
            with patch_dispatch_externals(), _silence_logs(), \
                 pinned_dispatch_authority(
                     db,
                     importer_lease,
                     cancellation_token=cancellation_token,
                 ) as (cancellation_token, owner_session_identity):
                outcome = dispatch_import_core(
                    make_dispatch_request(
                        path=source,
                        mb_release_id='generated-mbid',
                        request_id=42,
                        label='Generated Artist - Generated Album',
                        beets_harness_path=cfg.beets_harness_path,
                        dl_info=DownloadInfo(username='generated-user', filetype='mp3'),
                        attempt_spectral_audit=audit,
                        candidate_import_job_id=claimed.id,
                        beets_library_db_path=str(beets.library_db),
                        beets_library_root=str(beets.library_root),
                        execution_lease=importer_lease,
                        owner_session_identity=owner_session_identity,
                    ),
                    db,
                    cfg=cfg,
                    run_import_fn=run_import,
                    quality_gate_fn=quality_gate,
                    cancellation_token=cancellation_token,
                )
            finalize_claimed_dispatch(db, claimed, outcome)

    final_job = db.get_import_job(job.id)
    assert final_job is not None
    last_log = db.download_logs[-1] if db.download_logs else None
    return {
        "import_result": (
            last_log.import_result if last_log is not None else None
        ),
        "outcomes": [row.outcome for row in db.download_logs],
        "status": db.request(42)["status"],
        "active_automation_import_job_id": (
            db.request(42)["active_automation_import_job_id"]
        ),
        "validation_attempts": db.request(42)["validation_attempts"],
        "error_message": (
            last_log.error_message if last_log is not None else None
        ),
        "job_status": final_job.status,
        "denylist": [(row.username, row.reason) for row in db.denylist],
    }


def assert_automation_world_failure_self_heals(
    outcome: dict[str, object],
) -> None:
    """A launched automation ambiguity self-heals; it must never park.

    ``recovery_required`` behind ``processing`` is the removed policy: it left
    the request outside ``get_wanted``'s selection forever, with no terminal
    write for an operator to read. The current policy (#933, "nothing is ever
    parked") instead converts every ambiguous world into exactly one
    ``download_log`` audit row (so it reads in Recents), clears the exact
    processing owner, records a validation attempt so backoff keeps growing
    instead of resetting, and returns the request to ``wanted`` so the next
    cycle re-derives the truth.
    """
    from scripts import importer

    if outcome["job_status"] != "failed":
        raise AssertionError(
            "ambiguous automation completion parked at "
            f"{outcome['job_status']!r} instead of self-healing"
        )
    if outcome["status"] != "wanted":
        raise AssertionError(
            f"self-healed job left the request {outcome['status']!r} "
            "instead of wanted"
        )
    if outcome["active_automation_import_job_id"] is not None:
        raise AssertionError("self-heal left the automation owner attached")
    if outcome["validation_attempts"] != 1:
        raise AssertionError(
            "self-heal did not retain/record a validation attempt "
            f"(got {outcome['validation_attempts']!r})"
        )
    error_message = outcome["error_message"]
    message = error_message if isinstance(error_message, str) else ""
    if importer._WORLD_FAILURE_AUDIT_PREFIX not in message:
        raise AssertionError(
            "no download_log audit row recorded the world failure"
        )


class TestAttemptAuditCheckerQualification(unittest.TestCase):
    def test_finalization_checker_rejects_skipped_terminal_finalization(self):
        from lib.quality import ImportResult, SpectralAnalysisDetail, SpectralDetail

        audit = SpectralDetail(candidate=SpectralAnalysisDetail(
            attempted=True, grade="suspect", bitrate_kbps=96))
        skipped_finalization = ImportResult(decision="import").to_json()
        self.assertFalse(_persisted_attempt_has_exact_audit(
            skipped_finalization, audit))

    def test_have_provenance_checker_rejects_derivative_scan(self):
        from lib.quality import SpectralAnalysisDetail, SpectralDetail

        derivative_scan = SpectralDetail(
            existing=SpectralAnalysisDetail(
                attempted=True,
                grade="genuine",
            ),
        )
        self.assertFalse(_have_preserves_persisted_source(
            derivative_scan,
            expected_grade="likely_transcode",
            expected_bitrate=None,
            analyzer_calls=["candidate", "installed-opus"],
        ))

    def test_have_boundary_checker_rejects_blanket_persisted_mutant(self):
        self.assertFalse(_have_scan_boundary_holds(
            ["candidate"],
            preserve_existing_source=False,
            candidate_reused=False,
            reuse_have=False,
        ))

    def test_have_boundary_checker_rejects_blanket_scan_mutant(self):
        self.assertFalse(_have_scan_boundary_holds(
            ["candidate", "existing"],
            preserve_existing_source=True,
            candidate_reused=True,
            reuse_have=True,
        ))

    def test_have_boundary_checker_rejects_reused_candidate_rescan(self):
        self.assertFalse(_have_scan_boundary_holds(
            ["candidate", "existing"],
            preserve_existing_source=False,
            candidate_reused=True,
            reuse_have=True,
        ))

    def test_have_boundary_checker_rejects_reused_have_rescan(self):
        self.assertFalse(_have_scan_boundary_holds(
            ["existing"],
            preserve_existing_source=False,
            candidate_reused=True,
            reuse_have=True,
        ))

    def test_have_reuse_checker_rejects_error_grade_mutant(self):
        from lib.spectral_check import SPECTRAL_MEASUREMENT_VERSION

        self.assertFalse(_have_reuse_contract_holds(
            reuse_have=True,
            preserve_source=False,
            have_complete=True,
            snapshot_changed=False,
            persisted_grade="error",
            persisted_generation=SPECTRAL_MEASUREMENT_VERSION,
        ))

    def test_have_reuse_checker_rejects_old_generation_mutant(self):
        self.assertFalse(_have_reuse_contract_holds(
            reuse_have=True,
            preserve_source=False,
            have_complete=True,
            snapshot_changed=False,
            persisted_grade="suspect",
            persisted_generation=None,
        ))

    def test_have_reuse_checker_rejects_exact_generation_only_source_mutant(self):
        """#1007 known-bad: restoring PR #996's rule drops source history."""

        self.assertFalse(_have_reuse_contract_holds(
            reuse_have=False,
            preserve_source=True,
            have_complete=True,
            snapshot_changed=False,
            persisted_grade="likely_transcode",
            persisted_generation=None,
        ))

    def test_iron_and_wine_outer_checker_rejects_generation_only_mutant(self):
        with self.assertRaisesRegex(AssertionError, "policy dispatch"):
            assert_iron_and_wine_outer_policy(
                analyzer_roles=["candidate"],
                persisted_grade="likely_transcode",
                persisted_generation=None,
                dispatch_code="have_analysis_error",
                request_status="wanted",
                outcomes=["have_analysis_error"],
                canonical_source_removed=True,
                cleanup_receipt_recorded=True,
                job_terminal=True,
                owner_cleared=True,
            )

    def test_candidate_reuse_checker_rejects_matching_snapshot_rescan(self):
        with self.assertRaises(AssertionError):
            assert_candidate_snapshot_reuse(
                snapshot_changed=False,
                candidate_generation=SPECTRAL_MEASUREMENT_VERSION,
                has_have=False,
                full_preview_calls=0,
                analyzer_roles=["candidate"],
                candidate_status="reused",
                persisted_candidate_grade="genuine",
                persisted_have_grade=None,
                expected_candidate_grade="genuine",
            )

    def test_candidate_reuse_checker_rejects_changed_snapshot_skip(self):
        with self.assertRaises(AssertionError):
            assert_candidate_snapshot_reuse(
                snapshot_changed=True,
                candidate_generation=SPECTRAL_MEASUREMENT_VERSION,
                has_have=False,
                full_preview_calls=0,
                analyzer_roles=[],
                candidate_status="reused",
                persisted_candidate_grade="genuine",
                persisted_have_grade=None,
                expected_candidate_grade="genuine",
            )

    def test_candidate_reuse_checker_rejects_old_generation_skip(self):
        with self.assertRaises(AssertionError):
            assert_candidate_snapshot_reuse(
                snapshot_changed=False,
                candidate_generation=None,
                has_have=False,
                full_preview_calls=0,
                analyzer_roles=[],
                candidate_status="reused",
                persisted_candidate_grade="genuine",
                persisted_have_grade=None,
                expected_candidate_grade="genuine",
            )

    def test_world_failure_checker_rejects_planted_parked_outcome(self):
        """Known-bad: reviving the removed ``recovery_required`` park trips it."""
        with self.assertRaises(AssertionError):
            assert_automation_world_failure_self_heals({
                "job_status": "recovery_required",
                "status": "processing",
                "active_automation_import_job_id": 7,
                "validation_attempts": 0,
                "error_message": None,
            })

    def test_world_failure_checker_rejects_zeroed_retry_counters(self):
        """Known-bad: self-heal must retain/record the attempt, not reset it."""
        from scripts import importer

        with self.assertRaises(AssertionError):
            assert_automation_world_failure_self_heals({
                "job_status": "failed",
                "status": "wanted",
                "active_automation_import_job_id": None,
                "validation_attempts": 0,
                "error_message": f"{importer._WORLD_FAILURE_AUDIT_PREFIX}: boom",
            })

    def test_world_failure_checker_rejects_missing_audit_row(self):
        """Known-bad: a silent self-heal with no Recents-visible trace trips it."""
        with self.assertRaises(AssertionError):
            assert_automation_world_failure_self_heals({
                "job_status": "failed",
                "status": "wanted",
                "active_automation_import_job_id": None,
                "validation_attempts": 1,
                "error_message": "some unrelated failure",
            })


class TestAttemptAuditGenerated(unittest.TestCase):
    def setUp(self) -> None:
        repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        self.beets = BeetsWorld(repo_root)
        self.addCleanup(self.beets.close)
        self.runtime = patch.dict(os.environ, {
            "CRATEDIGGER_RUNTIME_CONFIG": str(
                self.beets.poisoned_runtime_config()
            ),
            "BEETS_DB": str(self.beets.root / "poisoned-library.db"),
        })
        self.runtime.start()
        self.addCleanup(self.runtime.stop)

    @given(
        job_mode=st.sampled_from(("automation", "force")),
        snapshot_changed=st.booleans(),
        candidate_generation=st.one_of(
            st.none(), st.integers(min_value=1, max_value=4),
        ),
        has_have=st.booleans(),
        candidate_grade=st.sampled_from((
            "genuine",
            "marginal",
            "suspect",
            "likely_transcode",
        )),
        track_count=st.integers(min_value=1, max_value=12),
    )
    @example(
        job_mode="force",
        snapshot_changed=False,
        candidate_generation=None,
        has_have=False,
        candidate_grade="genuine",
        track_count=12,
    )
    @example(
        job_mode="automation",
        snapshot_changed=False,
        candidate_generation=SPECTRAL_MEASUREMENT_VERSION,
        has_have=True,
        candidate_grade="suspect",
        track_count=1,
    )
    def test_preview_candidate_measurement_is_once_per_snapshot_across_job_modes(
        self,
        job_mode: PreviewJobMode,
        snapshot_changed: bool,
        candidate_generation: int | None,
        has_have: bool,
        candidate_grade: str,
        track_count: int,
    ):
        (
            full_preview_calls,
            analyzer_roles,
            candidate_status,
            persisted_candidate_grade,
            persisted_have_grade,
        ) = _run_candidate_snapshot_reuse_world(
            job_mode=job_mode,
            snapshot_changed=snapshot_changed,
            candidate_generation=candidate_generation,
            has_have=has_have,
            candidate_grade=candidate_grade,
            track_count=track_count,
        )
        assert_candidate_snapshot_reuse(
            snapshot_changed=snapshot_changed,
            candidate_generation=candidate_generation,
            has_have=has_have,
            full_preview_calls=full_preview_calls,
            analyzer_roles=analyzer_roles,
            candidate_status=candidate_status,
            persisted_candidate_grade=persisted_candidate_grade,
            persisted_have_grade=persisted_have_grade,
            expected_candidate_grade=candidate_grade,
        )

    def test_gespenst_mp3_reuses_exact_have_through_both_adapters(self):
        from lib.spectral_check import SPECTRAL_MEASUREMENT_VERSION

        (
            preserve_source,
            reuse_have,
            normal_calls,
            reused_calls,
            normal_audit,
            reused_audit,
        ) = _run_have_boundary_through_both_adapters(
            converted_from=None,
            lossless_v0_lineage=False,
            persisted_grade="genuine",
            persisted_bitrate=320,
            scanned_grade="suspect",
            scanned_bitrate=128,
            persisted_generation=SPECTRAL_MEASUREMENT_VERSION,
        )

        self.assertFalse(preserve_source)
        self.assertTrue(reuse_have)
        self.assertEqual(normal_calls, ["candidate"])
        self.assertEqual(reused_calls, [])
        for audit in (normal_audit, reused_audit):
            assert audit.existing is not None
            self.assertEqual(audit.existing.grade, "genuine")
            self.assertEqual(audit.existing.bitrate_kbps, 320)

    def test_family_in_the_armed_forces_remeasures_legacy_have(self):
        """Legacy suspect/128 HAVE cannot compare with a generation-2 peer."""

        from lib.spectral_check import SPECTRAL_MEASUREMENT_VERSION

        (
            preserve_source,
            reuse_have,
            normal_calls,
            reused_calls,
            normal_audit,
            reused_audit,
        ) = _run_have_boundary_through_both_adapters(
            converted_from=None,
            lossless_v0_lineage=False,
            persisted_grade="suspect",
            persisted_bitrate=128,
            persisted_generation=None,
            scanned_grade="suspect",
            scanned_bitrate=128,
        )

        self.assertFalse(preserve_source)
        self.assertFalse(reuse_have)
        self.assertEqual(normal_calls, ["candidate", "existing"])
        self.assertEqual(reused_calls, ["existing"])
        for audit in (normal_audit, reused_audit):
            assert audit.existing is not None
            self.assertEqual(audit.existing.grade, "suspect")
            self.assertEqual(audit.existing.bitrate_kbps, 128)
            self.assertEqual(
                audit.existing.spectral_measurement_version,
                SPECTRAL_MEASUREMENT_VERSION,
            )

    @given(
        persisted_generation=st.one_of(
            st.none(),
            st.integers(min_value=0, max_value=1),
            st.integers(min_value=3, max_value=5),
            st.just(SPECTRAL_MEASUREMENT_VERSION),
        ),
        persisted_grade=st.sampled_from((
            "genuine", "marginal", "suspect", "likely_transcode",
        )),
        installed=st.sampled_from((
            ("mp3", "MP3"),
            ("opus", "Opus"),
            ("m4a", "AAC"),
            # Beets maps its OGG format label to this canonical codec label.
            ("ogg", "vorbis"),
        )),
    )
    @example(
        persisted_generation=None,
        persisted_grade="likely_transcode",
        installed=("ogg", "vorbis"),
    )
    @example(
        persisted_generation=SPECTRAL_MEASUREMENT_VERSION + 1,
        persisted_grade="suspect",
        installed=("mp3", "MP3"),
    )
    def test_preserved_source_generations_reach_both_preview_adapters(
        self,
        persisted_generation: int | None,
        persisted_grade: str,
        installed: tuple[str, str],
    ):
        """#1007 spans NULL, old/current/future at both preview adapters."""

        (
            preserve_source,
            reuse_have,
            normal_calls,
            reused_calls,
            normal_audit,
            reused_audit,
        ) = _run_have_boundary_through_both_adapters(
            converted_from="flac",
            lossless_v0_lineage=True,
            persisted_grade=persisted_grade,
            persisted_bitrate=232,
            persisted_generation=persisted_generation,
            scanned_grade="genuine",
            scanned_bitrate=320,
            candidate_grade="likely_transcode",
            installed=installed,
        )

        self.assertTrue(preserve_source)
        self.assertTrue(_have_reuse_contract_holds(
            reuse_have=reuse_have,
            preserve_source=preserve_source,
            have_complete=True,
            snapshot_changed=False,
            persisted_grade=persisted_grade,
            persisted_generation=persisted_generation,
        ))
        self.assertTrue(reuse_have)
        self.assertEqual(normal_calls, ["candidate"])
        self.assertEqual(reused_calls, [])
        for audit in (normal_audit, reused_audit):
            assert audit.existing is not None
            self.assertEqual(audit.existing.grade, persisted_grade)
            self.assertEqual(
                audit.existing.spectral_measurement_version,
                persisted_generation,
            )

    @given(
        native=st.sampled_from((
            ("flac", "FLAC"),
            # The real ALAC case is an M4A container, not a made-up .alac
            # extension. Its storage/measurement codec disambiguates it.
            ("m4a", "ALAC"),
            ("wav", "WAV"),
        )),
        persisted_generation=st.sampled_from((
            None,
            SPECTRAL_MEASUREMENT_VERSION - 1,
            SPECTRAL_MEASUREMENT_VERSION,
            SPECTRAL_MEASUREMENT_VERSION + 1,
        )),
    )
    @example(native=("m4a", "ALAC"), persisted_generation=None)
    def test_native_lossless_source_anchors_remain_generation_strict(
        self,
        native: tuple[str, str],
        persisted_generation: int | None,
    ) -> None:
        """Proof/V0 provenance cannot suppress a native HAVE remeasurement."""

        from lib.current_library_evidence import current_spectral_evidence_reusable
        from lib.quality import (
            AlbumQualityEvidenceFile,
            AlbumQualityV0Metric,
            AudioQualityMeasurement,
            VerifiedLosslessProof,
        )
        from lib.quality_evidence import (
            current_evidence_for_policy,
            current_evidence_preserves_source_spectral,
        )
        from tests.evidence_helpers import make_album_quality_evidence

        extension, storage_format = native
        evidence = make_album_quality_evidence(
            preserve_spectral_measurement_version=True,
            files=[AlbumQualityEvidenceFile(
                relative_path=f"01.{extension}",
                size_bytes=1,
                mtime_ns=1,
                extension=extension,
                container=extension,
                codec=extension,
            )],
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=700,
                avg_bitrate_kbps=750,
                median_bitrate_kbps=725,
                format=storage_format,
                spectral_grade="likely_transcode",
                spectral_bitrate_kbps=96,
                spectral_subject="source",
                spectral_provenance="carried",
                spectral_measurement_version=persisted_generation,
                was_converted_from="flac",
            ),
            v0_metric=AlbumQualityV0Metric(
                min_bitrate_kbps=165,
                avg_bitrate_kbps=171,
                median_bitrate_kbps=168,
                subject="source",
                provenance="carried",
            ),
            verified_lossless_proof=VerifiedLosslessProof(
                provenance="carried",
                source="flac",
                classifier="spectral_verified_lossless",
            ),
            codec=extension,
            container=extension,
            storage_format=storage_format,
        )

        self.assertFalse(current_evidence_preserves_source_spectral(evidence))
        reusable = current_spectral_evidence_reusable(evidence)
        self.assertEqual(
            reusable,
            persisted_generation == SPECTRAL_MEASUREMENT_VERSION,
        )
        projected = current_evidence_for_policy(evidence)
        if reusable:
            self.assertEqual(
                projected.measurement.spectral_grade,
                "likely_transcode",
            )
        else:
            self.assertIsNone(projected.measurement.spectral_grade)

    @given(
        persisted_generation=st.one_of(
            st.none(), st.integers(min_value=0, max_value=5),
        ),
        invalid_grade=st.sampled_from(("", "error", "unknown")),
    )
    def test_preserved_source_invalid_grades_never_become_reusable(
        self,
        persisted_generation: int | None,
        invalid_grade: str,
    ):
        """The generation exception admits recognised grades only."""

        from lib.current_library_evidence import current_spectral_evidence_reusable
        from lib.quality import AudioQualityMeasurement
        from lib.quality_evidence import current_evidence_for_policy
        from tests.evidence_helpers import make_album_quality_evidence

        evidence = make_album_quality_evidence(
            preserve_spectral_measurement_version=True,
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=123,
                avg_bitrate_kbps=123,
                median_bitrate_kbps=123,
                format="Opus",
                spectral_grade=invalid_grade,
                spectral_subject="source",
                spectral_provenance="carried",
                spectral_measurement_version=persisted_generation,
                was_converted_from="flac",
            ),
            codec="opus",
            container="opus",
            storage_format="Opus",
        )

        self.assertFalse(current_spectral_evidence_reusable(evidence))
        projected = current_evidence_for_policy(evidence)
        self.assertIsNone(projected.measurement.spectral_grade)
        self.assertIsNone(projected.measurement.spectral_subject)

    @given(
        have_complete=st.booleans(),
        snapshot_changed=st.booleans(),
        persisted_grade=st.sampled_from((
            "", "error", "genuine", "marginal", "suspect",
            "likely_transcode",
        )),
        persisted_bitrate=st.one_of(
            st.none(), st.integers(min_value=32, max_value=400),
        ),
        scanned_grade=st.sampled_from((
            "genuine", "suspect", "likely_transcode",
        )),
        scanned_bitrate=st.one_of(
            st.none(), st.integers(min_value=32, max_value=400),
        ),
        persisted_generation=st.one_of(
            st.none(), st.integers(min_value=1, max_value=4),
        ),
    )
    @example(
        have_complete=True,
        snapshot_changed=False,
        persisted_grade="genuine",
        persisted_bitrate=192,
        scanned_grade="suspect",
        scanned_bitrate=96,
        persisted_generation=None,
    )
    @example(
        have_complete=False,
        snapshot_changed=False,
        persisted_grade="genuine",
        persisted_bitrate=192,
        scanned_grade="suspect",
        scanned_bitrate=96,
        persisted_generation=2,
    )
    @example(
        have_complete=True,
        snapshot_changed=True,
        persisted_grade="genuine",
        persisted_bitrate=192,
        scanned_grade="suspect",
        scanned_bitrate=96,
        persisted_generation=2,
    )
    @example(
        have_complete=True,
        snapshot_changed=False,
        persisted_grade="error",
        persisted_bitrate=None,
        scanned_grade="genuine",
        scanned_bitrate=192,
        persisted_generation=2,
    )
    def test_preview_have_reuse_requires_complete_matching_snapshot(
        self,
        have_complete: bool,
        snapshot_changed: bool,
        persisted_grade: str,
        persisted_bitrate: int | None,
        scanned_grade: str,
        scanned_bitrate: int | None,
        persisted_generation: int | None,
    ):
        from lib.spectral_check import SPECTRAL_MEASUREMENT_VERSION

        (
            preserve_source,
            reuse_have,
            normal_calls,
            reused_calls,
            normal_audit,
            reused_audit,
        ) = _run_have_boundary_through_both_adapters(
            converted_from=None,
            lossless_v0_lineage=False,
            persisted_grade=persisted_grade,
            persisted_bitrate=persisted_bitrate,
            scanned_grade=scanned_grade,
            scanned_bitrate=scanned_bitrate,
            persisted_generation=persisted_generation,
            have_complete=have_complete,
            snapshot_changed=snapshot_changed,
        )

        expected_reuse = (
            have_complete
            and not snapshot_changed
            and persisted_generation == SPECTRAL_MEASUREMENT_VERSION
            and persisted_grade in {
                "genuine",
                "marginal",
                "suspect",
                "likely_transcode",
            }
        )
        self.assertFalse(preserve_source)
        self.assertTrue(_have_reuse_contract_holds(
            reuse_have=reuse_have,
            preserve_source=preserve_source,
            have_complete=have_complete,
            snapshot_changed=snapshot_changed,
            persisted_grade=persisted_grade,
            persisted_generation=persisted_generation,
        ))
        self.assertEqual(reuse_have, expected_reuse)
        self.assertEqual(
            normal_calls,
            ["candidate"] if expected_reuse else ["candidate", "existing"],
        )
        self.assertEqual(
            reused_calls,
            [] if expected_reuse else ["existing"],
        )
        expected_grade = (
            persisted_grade if expected_reuse else scanned_grade
        )
        expected_bitrate = (
            persisted_bitrate if expected_reuse else scanned_bitrate
        )
        for audit in (normal_audit, reused_audit):
            assert audit.existing is not None
            self.assertEqual(audit.existing.grade, expected_grade)
            self.assertEqual(audit.existing.bitrate_kbps, expected_bitrate)

    def test_authoritative_evidence_checker_rejects_scalar_fallback_mutant(self):
        req = {
            "current_spectral_grade": "likely_transcode",
            "current_spectral_bitrate": 224,
        }

        self.assertFalse(_authoritative_have_matches(
            _stale_scalar_fallback_mutant(req),
            None,
            None,
        ))

    @given(
        authoritative_grade=st.one_of(
            st.none(),
            st.sampled_from(("genuine", "suspect", "likely_transcode")),
        ),
        authoritative_bitrate=st.one_of(
            st.none(), st.integers(min_value=32, max_value=400)),
        stale_grade=st.sampled_from(
            ("genuine", "suspect", "likely_transcode")),
        stale_bitrate=st.integers(min_value=32, max_value=400),
    )
    def test_current_evidence_dominates_stale_request_scalars(
        self,
        authoritative_grade,
        authoritative_bitrate,
        stale_grade,
        stale_bitrate,
    ):
        from lib.current_library_evidence import (
            CurrentLibraryEvidence,
            resolve_current_library_evidence,
        )
        from lib.quality import AudioQualityMeasurement, QualityRankConfig
        from lib.quality_evidence import EvidenceBuildResult
        from tests.evidence_helpers import make_album_quality_evidence
        from tests.fakes import FakePipelineDB
        from tests.helpers import make_request_row

        db = FakePipelineDB()
        req = make_request_row(
            id=42,
            current_spectral_grade=stale_grade,
            current_spectral_bitrate=stale_bitrate,
        )
        db.seed_request(req)
        evidence_spectral_bitrate = (
            authoritative_bitrate
            if authoritative_grade is not None
            else None
        )
        evidence = make_album_quality_evidence(
            mb_release_id=req["mb_release_id"],
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=122,
                avg_bitrate_kbps=127,
                median_bitrate_kbps=127,
                format="Opus",
                spectral_grade=authoritative_grade,
                spectral_bitrate_kbps=evidence_spectral_bitrate,
                spectral_subject=(
                    "installed" if authoritative_grade is not None else None
                ),
                spectral_provenance=(
                    "measured" if authoritative_grade is not None else None
                ),
            ),
            codec="opus",
            container="opus",
            storage_format="Opus",
        )
        db.upsert_album_quality_evidence(evidence)
        persisted = db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert persisted is not None and persisted.id is not None
        db.set_request_current_evidence(42, persisted.id)

        # Drive the resolver every lane actually calls, not the loader
        # underneath it: until issue #1313 this property read a tuple slot
        # the resolver reassigned in all three of its branches before ever
        # looking at it, so no production HAVE projection was under test.
        resolved = resolve_current_library_evidence(
            db,
            request_id=42,
            mb_release_id=req["mb_release_id"],
            quality_ranks=QualityRankConfig.defaults(),
            beets_library_root="/library",
            loader=lambda *_a, **_k: EvidenceBuildResult(persisted, "ready"),
        )

        self.assertIsInstance(resolved, CurrentLibraryEvidence)
        assert isinstance(resolved, CurrentLibraryEvidence)
        self.assertTrue(_authoritative_have_matches(
            resolved.existing_spectral_evidence,
            authoritative_grade,
            evidence_spectral_bitrate,
        ))

    @given(
        converted_from=st.one_of(
            st.none(),
            st.sampled_from((
                "flac", "FLAC", "alac", "wav", "m4a", "mp3", "aac",
                "opus",
            )),
        ),
        lossless_v0_lineage=st.booleans(),
        persisted_grade=st.sampled_from((
            "genuine", "suspect", "likely_transcode",
        )),
        persisted_bitrate=st.one_of(
            st.none(), st.integers(min_value=32, max_value=400),
        ),
        scanned_grade=st.sampled_from((
            "genuine", "suspect", "likely_transcode",
        )),
        scanned_bitrate=st.one_of(
            st.none(), st.integers(min_value=32, max_value=400),
        ),
    )
    def test_have_scan_boundary_matches_lossless_conversion_provenance(
        self,
        converted_from: str | None,
        lossless_v0_lineage: bool,
        persisted_grade: str,
        persisted_bitrate: int | None,
        scanned_grade: str,
        scanned_bitrate: int | None,
    ):
        from lib.quality import LOSSLESS_CODECS

        (
            preserve_existing_source,
            reuse_have,
            normal_calls,
            reused_calls,
            normal_audit,
            reused_audit,
        ) = _run_have_boundary_through_both_adapters(
            converted_from=converted_from,
            lossless_v0_lineage=lossless_v0_lineage,
            persisted_grade=persisted_grade,
            persisted_bitrate=persisted_bitrate,
            scanned_grade=scanned_grade,
            scanned_bitrate=scanned_bitrate,
            persisted_generation=SPECTRAL_MEASUREMENT_VERSION,
        )
        # Source anchors describe provenance only.  The scan exemption needs
        # both a lossless conversion record and an installed lossy derivative.
        self.assertEqual(
            preserve_existing_source,
            (
                (converted_from or "").lower() in LOSSLESS_CODECS
            ),
        )
        self.assertTrue(_have_scan_boundary_holds(
            normal_calls,
            preserve_existing_source=preserve_existing_source,
            candidate_reused=False,
            reuse_have=reuse_have,
        ))
        self.assertTrue(_have_scan_boundary_holds(
            reused_calls,
            preserve_existing_source=preserve_existing_source,
            candidate_reused=True,
            reuse_have=reuse_have,
        ))
        self.assertTrue(reuse_have)
        for audit in (normal_audit, reused_audit):
            assert audit.existing is not None
            self.assertEqual(audit.existing.grade, persisted_grade)
            self.assertEqual(audit.existing.bitrate_kbps, persisted_bitrate)

    @given(
        mode=st.sampled_from((
            "success", "rejection", "no_json", "timeout",
            "pre_result_exception", "post_result_exception",
            "manifest_rejection",
        )),
        new_bitrate=st.integers(min_value=64, max_value=400),
        existing_bitrate=st.integers(min_value=64, max_value=400),
        converted=st.booleans(),
        audit_grade=st.sampled_from(("genuine", "suspect", "likely_transcode")),
        audit_bitrate=st.one_of(st.none(), st.integers(min_value=32, max_value=400)),
    )
    @example(
        mode="no_json",
        new_bitrate=64,
        existing_bitrate=64,
        converted=False,
        audit_grade="genuine",
        audit_bitrate=None,
    )
    @example(
        mode="success",
        new_bitrate=64,
        existing_bitrate=64,
        converted=False,
        audit_grade="genuine",
        audit_bitrate=None,
    )
    def test_real_dispatch_finalization_preserves_audit_without_policy_drift(
        self,
        mode: str,
        new_bitrate: int,
        existing_bitrate: int,
        converted: bool,
        audit_grade: str,
        audit_bitrate: int | None,
    ):
        from lib.quality import SpectralAnalysisDetail, SpectralDetail

        audit = SpectralDetail(
            candidate=SpectralAnalysisDetail(
                attempted=True, grade=audit_grade, bitrate_kbps=audit_bitrate),
            existing=SpectralAnalysisDetail(
                attempted=True, grade="genuine", bitrate_kbps=existing_bitrate),
        )
        audited = _run_dispatch_finalization_world(
            mode=mode,
            audit=audit,
            new_bitrate=new_bitrate,
            existing_bitrate=existing_bitrate,
            converted=converted,
            beets=self.beets,
        )
        unaudited = _run_dispatch_finalization_world(
            mode=mode,
            audit=None,
            new_bitrate=new_bitrate,
            existing_bitrate=existing_bitrate,
            converted=converted,
            beets=self.beets,
        )

        ambiguous_modes = {
            "no_json", "timeout", "pre_result_exception",
            "post_result_exception",
        }
        if mode in ambiguous_modes:
            # #933: ambiguous automation completions no longer park as
            # ``recovery_required`` behind a stuck ``processing`` request.
            # They self-heal in the same frame the crash/timeout/no-JSON
            # ambiguity is discovered.
            self.assertIsNone(audited["import_result"])
            self.assertIsNone(unaudited["import_result"])
            assert_automation_world_failure_self_heals(audited)
            assert_automation_world_failure_self_heals(unaudited)
        else:
            self.assertTrue(_persisted_attempt_has_exact_audit(
                audited["import_result"], audit))
            self.assertEqual(
                _policy_payload(audited["import_result"]),
                _policy_payload(unaudited["import_result"]),
            )
        self.assertEqual(audited["outcomes"], unaudited["outcomes"])
        self.assertEqual(audited["status"], unaudited["status"])
        self.assertEqual(audited["job_status"], unaudited["job_status"])
        self.assertEqual(audited["denylist"], unaudited["denylist"])

    @given(
        new_bitrate=st.integers(min_value=64, max_value=400),
        existing_bitrate=st.integers(min_value=64, max_value=400),
        audit_grade=st.sampled_from(["genuine", "suspect", "likely_transcode"]),
        audit_floor=st.one_of(st.none(), st.integers(min_value=64, max_value=320)),
        candidate_fails=st.booleans(),
        existing_fails=st.booleans(),
    )
    def test_arbitrary_audit_cannot_change_policy_result_at_dispatch_adapter(
        self, new_bitrate: int, existing_bitrate: int,
        audit_grade: str, audit_floor: int | None,
        candidate_fails: bool, existing_fails: bool,
    ):
        from lib.dispatch.types import ImportAttemptResult
        from lib.quality import (
            AudioQualityMeasurement,
            ImportResult,
            SpectralAnalysisDetail,
            SpectralDetail,
        )

        result = ImportResult(
            decision="import" if new_bitrate > existing_bitrate else "downgrade",
            source_measurement=AudioQualityMeasurement(
                min_bitrate_kbps=new_bitrate, avg_bitrate_kbps=new_bitrate,
                median_bitrate_kbps=new_bitrate, format="MP3"),
            current_measurement=AudioQualityMeasurement(
                min_bitrate_kbps=existing_bitrate,
                avg_bitrate_kbps=existing_bitrate,
                median_bitrate_kbps=existing_bitrate, format="MP3"),
        )
        audit = SpectralDetail(
            candidate=SpectralAnalysisDetail(
                attempted=True, grade=None if candidate_fails else audit_grade,
                bitrate_kbps=None if candidate_fails else audit_floor,
                error="candidate failed" if candidate_fails else None),
            existing=SpectralAnalysisDetail(
                attempted=True, grade=None if existing_fails else audit_grade,
                bitrate_kbps=None if existing_fails else audit_floor,
                error="existing failed" if existing_fails else None),
        )
        before = _policy_snapshot(result)
        attached = ImportAttemptResult(audit).merge(result)
        self.assertTrue(_policy_snapshot_unchanged(before, attached))
        self.assertIs(attached.spectral, audit)

    def test_policy_snapshot_checker_rejects_planted_adapter_mutant(self):
        from lib.quality import (
            AudioQualityMeasurement,
            ImportResult,
            SpectralAnalysisDetail,
            SpectralDetail,
        )

        result = ImportResult(
            decision="import",
            source_measurement=AudioQualityMeasurement(
                min_bitrate_kbps=320, avg_bitrate_kbps=320,
                median_bitrate_kbps=320, format="MP3"),
        )
        audit = SpectralDetail(candidate=SpectralAnalysisDetail(
            attempted=True, grade="likely_transcode", bitrate_kbps=96))
        before = _policy_snapshot(result)
        mutant = ImportResult(
            decision=result.decision,
            source_measurement=AudioQualityMeasurement(
                min_bitrate_kbps=96, avg_bitrate_kbps=96,
                median_bitrate_kbps=96, format="MP3",
                spectral_grade=audit.candidate.grade if audit.candidate else None,
            ),
            spectral=audit,
        )
        self.assertFalse(_policy_snapshot_unchanged(before, mutant))


@requires_postgres
class TestIronAndWineOuterEvidenceSlice(unittest.TestCase):
    """#1007's live witness through preview persistence and automation import."""

    def test_preserved_wrong_generation_reaches_real_dispatch_policy(self):
        from lib.beets_db import AlbumInfo
        from lib.config import CratediggerConfig
        from lib.dispatch import dispatch_import_core
        from lib.dispatch.types import DispatchOutcome, DispatchRequest, ImportOneRun
        from lib.download_processing import CompletionDispatched
        from lib.download_reconstruction import reconstruct_grab_list_entry
        from lib.import_execution import (
            CancellationToken,
            ExecutionLeaseSnapshot,
            ExecutionOwnerProof,
            OwnerSessionIdentity,
            ProcessIdentity,
        )
        from lib.import_preview import (
            HeaderRepairFn,
            ImportPreviewDB,
            ImportPreviewResult,
        )
        from lib.import_queue import ImportJob
        from lib.measurement import ExistingSpectralAuditLookup
        from lib.pipeline_db import PipelineDB
        from lib.processing_paths import canonical_folder_for_row
        from lib.quality import (
            EVIDENCE_SUBJECT_SOURCE,
            ActiveDownloadFileState,
            ActiveDownloadState,
            AlbumQualityV0Metric,
            AudioQualityMeasurement,
            DownloadInfo,
            ImportResult,
            SpectralAnalysisDetail,
            V0ProbeEvidence,
        )
        from lib.quality_evidence import snapshot_audio_files
        from scripts import import_preview_worker
        from scripts.importer import execute_automation_import_job, process_claimed_job
        from tests.dispatch_helpers import (
            handoff_automation_owner,
            make_dispatch_request,
        )
        from tests.evidence_helpers import make_album_quality_evidence
        from tests.fakes import FakeBeetsDB

        db = make_db()
        self.addCleanup(db.close)
        with tempfile.TemporaryDirectory() as root:
            staging_dir = os.path.join(root, "Incoming")
            existing = os.path.join(root, "Beets", "Iron & Wine")
            processing_dir = os.path.join(root, "processing")
            os.makedirs(existing)
            os.makedirs(os.path.join(root, "slskd"))
            os.makedirs(os.path.join(processing_dir, "albums"), mode=0o700)
            os.makedirs(os.path.join(processing_dir, "preview"), mode=0o700)
            os.chmod(processing_dir, 0o700)
            for number in (1, 2, 3):
                Path(existing, f"{number:02d}.opus").write_bytes(
                    b"installed-opus",
                )
            cfg = CratediggerConfig(
                audio_check_mode="off",
                beets_harness_path="/fake/harness/run_beets_harness.sh",
                beets_staging_dir=staging_dir,
                slskd_download_dir=os.path.join(root, "slskd"),
                processing_dir=processing_dir,
                pipeline_db_enabled=True,
            )
            mbid = "iron-and-wine-creek"
            request_id = db.add_request(
                "Iron & Wine",
                "The Creek Drank the Cradle",
                "request",
                mb_release_id=mbid,
            )
            db.set_tracks(request_id, [{"track_number": 1, "title": "Track"}])
            state = ActiveDownloadState(
                filetype="flac",
                enqueued_at="2026-08-03T00:00:00+00:00",
                files=[ActiveDownloadFileState(
                    username="rexasaurus",
                    filename="Iron & Wine\\The Creek Drank the Cradle\\01.flac",
                    file_dir="Iron & Wine\\The Creek Drank the Cradle",
                    size=len(b"candidate-flac"),
                )],
            )
            request = db.get_request(request_id)
            assert request is not None
            candidate = canonical_folder_for_row(
                reconstruct_grab_list_entry(request, state),
                os.path.join(processing_dir, "albums"),
            )
            state = msgspec.structs.replace(state, current_path=candidate)
            os.makedirs(candidate)
            Path(candidate, "01.flac").write_bytes(b"candidate-flac")
            current = make_album_quality_evidence(
                preserve_spectral_measurement_version=True,
                mb_release_id=mbid,
                source_path=existing,
                files=snapshot_audio_files(existing),
                measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=114,
                    avg_bitrate_kbps=123,
                    median_bitrate_kbps=123,
                    format="Opus",
                    spectral_grade="likely_transcode",
                    spectral_bitrate_kbps=96,
                    spectral_subject=EVIDENCE_SUBJECT_SOURCE,
                    spectral_provenance="carried",
                    spectral_measurement_version=None,
                    was_converted_from="flac",
                ),
                v0_metric=AlbumQualityV0Metric(
                    min_bitrate_kbps=223,
                    avg_bitrate_kbps=232,
                    median_bitrate_kbps=228,
                    subject=EVIDENCE_SUBJECT_SOURCE,
                    provenance="carried",
                ),
                on_disk_v0_research_attempted=True,
                codec="opus",
                container="opus",
                storage_format="Opus",
            )
            db.upsert_album_quality_evidence(current)
            persisted_current = db.find_album_quality_evidence(
                mb_release_id=mbid,
                snapshot_fingerprint=current.snapshot_fingerprint,
            )
            assert persisted_current is not None and persisted_current.id is not None
            self.assertEqual(persisted_current.measurement.min_bitrate_kbps, 114)
            self.assertEqual(persisted_current.measurement.avg_bitrate_kbps, 123)
            self.assertEqual(
                persisted_current.measurement.spectral_grade,
                "likely_transcode",
            )
            assert persisted_current.v0_metric is not None
            self.assertEqual(persisted_current.v0_metric.min_bitrate_kbps, 223)
            self.assertEqual(persisted_current.v0_metric.avg_bitrate_kbps, 232)
            self.assertTrue(db.set_request_current_evidence(
                request_id, persisted_current.id,
            ))
            job = handoff_automation_owner(
                db,
                request_id,
                state=state.to_json(),
                canonical_path=candidate,
            )
            preview_lease = ExecutionLeaseSnapshot(
                host_boot_id="iron-and-wine-preview-boot",
                invocation_id="iron-and-wine-preview",
                systemd_unit="cratedigger-import-preview-worker.service",
                worker=ProcessIdentity(pid=7001, start_ticks=70001),
            )
            claimed_preview = claim_next_import_preview_job(
                db,
                worker_id="iron-and-wine-preview",
                execution_lease=preview_lease,
            )
            assert claimed_preview is not None and claimed_preview.id == job.id
            beets = FakeBeetsDB()
            beets.set_album_info(mbid, AlbumInfo(
                album_id=1,
                track_count=3,
                min_bitrate_kbps=114,
                avg_bitrate_kbps=123,
                median_bitrate_kbps=123,
                is_cbr=False,
                album_path=existing,
                format="Opus",
            ))
            analyzer_roles: list[str] = []

            def analyze(path: str) -> SpectralAnalysisDetail:
                analyzer_roles.append(
                    "existing" if path == existing else "candidate",
                )
                return SpectralAnalysisDetail(
                    attempted=True,
                    grade="likely_transcode",
                    bitrate_kbps=96,
                    spectral_measurement_version=SPECTRAL_MEASUREMENT_VERSION,
                )

            def run_preview_import(**_kwargs: object) -> ImportOneRun:
                return ImportOneRun(
                    command=("import_one",),
                    returncode=0,
                    stdout="",
                    stderr="",
                    import_result=ImportResult(
                        decision="import",
                        source_measurement=AudioQualityMeasurement(
                            min_bitrate_kbps=472,
                            avg_bitrate_kbps=506,
                            median_bitrate_kbps=500,
                            format="FLAC",
                            spectral_grade="likely_transcode",
                            spectral_bitrate_kbps=96,
                            spectral_subject=EVIDENCE_SUBJECT_SOURCE,
                            spectral_provenance="measured",
                            spectral_measurement_version=(
                                SPECTRAL_MEASUREMENT_VERSION
                            ),
                        ),
                        v0_probe=V0ProbeEvidence(
                            kind="lossless_source_v0",
                            min_bitrate_kbps=165,
                            avg_bitrate_kbps=171,
                            median_bitrate_kbps=168,
                        ),
                    ),
                )

            def measure_candidate(
                db_arg: ImportPreviewDB,
                *,
                request_id: int,
                path: str,
                source_display_path: str | None = None,
                force: bool = False,
                download_log_id: int | None = None,
                import_job_id: int | None = None,
                runtime_config: CratediggerConfig | None = None,
                repair_fn: HeaderRepairFn | None = None,
                cancellation_token: CancellationToken | None = None,
            ) -> ImportPreviewResult:
                del cancellation_token
                return import_preview_worker.measure_and_persist_candidate_evidence(
                    db_arg,
                    request_id=request_id,
                    path=path,
                    source_display_path=source_display_path,
                    force=force,
                    download_log_id=download_log_id,
                    import_job_id=import_job_id,
                    runtime_config=runtime_config or cfg,
                    repair_fn=repair_fn,
                    run_import_fn=run_preview_import,
                    spectral_detail_analyzer=analyze,
                    existing_spectral_resolver=lambda _release_id: (
                        ExistingSpectralAuditLookup(path=existing)
                    ),
                )

            def execute_owned_automation(
                db_arg: PipelineDB,
                job_arg: ImportJob,
                *,
                ctx: object,
                execution_lease: ExecutionLeaseSnapshot,
                cancellation_token: CancellationToken,
                owner_session_identity: OwnerSessionIdentity,
            ) -> DispatchOutcome:
                del ctx
                return execute_automation_import_job(
                    db_arg,
                    job_arg,
                    ctx=object(),
                    completed_processing_fn=completed_processing,
                    execution_lease=execution_lease,
                    cancellation_token=cancellation_token,
                    owner_session_identity=owner_session_identity,
                )

            with (
                patch("lib.beets_db.BeetsDB", lambda *_args, **_kwargs: beets),
                db._pin_owner_session(
                    preview_token := CancellationToken(),
                ) as preview_owner_session,
            ):
                preview_authority = (
                    import_preview_worker._automation_authority_snapshot(
                        db,
                        claimed_preview,
                        preview_lease,
                        runtime_config=cfg,
                    )
                )
                assert preview_authority is not None
                updated = import_preview_worker.process_claimed_preview_job(
                    db,
                    claimed_preview,
                    spectral_detail_analyzer=analyze,
                    existing_spectral_resolver=lambda _release_id: (
                        ExistingSpectralAuditLookup(path=existing)
                    ),
                    runtime_config=cfg,
                    execution_lease=preview_lease,
                    automation_authority=preview_authority,
                    cancellation_token=preview_token,
                    owner_session_identity=preview_owner_session,
                    candidate_measurement_fn=measure_candidate,
                )
            assert updated is not None and updated.preview_result is not None
            self.assertEqual(
                updated.preview_status,
                "evidence_ready",
                updated.preview_result,
            )
            candidate_evidence_id = db.get_import_job_candidate_evidence_id(job.id)
            persisted_candidate = db.load_album_quality_evidence_by_id(
                candidate_evidence_id,
            )
            assert persisted_candidate is not None
            self.assertEqual(
                persisted_candidate.measurement.spectral_subject, "source",
            )
            self.assertEqual(
                persisted_candidate.measurement.spectral_provenance, "measured",
            )
            self.assertEqual(
                persisted_candidate.measurement.spectral_measurement_version,
                SPECTRAL_MEASUREMENT_VERSION,
            )
            self.assertEqual(persisted_candidate.measurement.min_bitrate_kbps, 472)
            self.assertEqual(persisted_candidate.measurement.avg_bitrate_kbps, 506)
            self.assertEqual(
                persisted_candidate.measurement.spectral_grade,
                "likely_transcode",
            )
            self.assertEqual(
                persisted_candidate.measurement.spectral_bitrate_kbps,
                96,
            )
            assert persisted_candidate.v0_metric is not None
            self.assertEqual(persisted_candidate.v0_metric.min_bitrate_kbps, 165)
            self.assertEqual(persisted_candidate.v0_metric.avg_bitrate_kbps, 171)
            preview_import = ImportResult.from_dict(
                updated.preview_result["import_result"],
            )
            assert preview_import.spectral.existing is not None
            self.assertEqual(
                preview_import.spectral.existing.grade,
                "likely_transcode",
            )
            self.assertIsNone(
                preview_import.spectral.existing.spectral_measurement_version,
            )
            importer_lease = ExecutionLeaseSnapshot(
                host_boot_id="iron-and-wine-importer-boot",
                invocation_id="iron-and-wine-importer",
                systemd_unit="cratedigger-importer.service",
                worker=ProcessIdentity(pid=7002, start_ticks=70002),
            )
            claimed_importer = claim_next_import_job(
                db,
                worker_id="iron-and-wine-importer",
                execution_lease=importer_lease,
            )
            assert claimed_importer is not None and claimed_importer.id == job.id

            # Batch F, F3 (issue #1355 residual triage round 2): the
            # identity half of ``owner_proof`` reaches real production
            # reverification and fails closed on a mismatch, but the
            # lease half had no reader at all, so a stub that dropped it
            # (``execution_lease=None``) survived every assertion below.
            # Capturing the built request and asserting its
            # ``execution_lease`` constrains the lease half the same way.
            dispatched_requests: list[DispatchRequest] = []

            def completed_processing(
                _entry: object,
                _state: object,
                _ctx: object,
                *,
                import_job_id: int,
                cancellation_token: CancellationToken,
                owner_proof: ExecutionOwnerProof,
                **_kwargs: object,
            ) -> CompletionDispatched:
                dispatch_request = make_dispatch_request(
                    path=candidate,
                    mb_release_id=mbid,
                    request_id=request_id,
                    label='Iron & Wine - The Creek Drank the Cradle',
                    beets_harness_path=cfg.beets_harness_path,
                    dl_info=DownloadInfo(username='rexasaurus', filetype='flac'),
                    distance=0.05,
                    candidate_import_job_id=import_job_id,
                    execution_lease=owner_proof.execution_lease,
                    owner_session_identity=owner_proof.owner_session_identity,
                )
                dispatched_requests.append(dispatch_request)
                return CompletionDispatched(dispatch_import_core(
                    dispatch_request,
                    db,
                    cfg=cfg,
                    cancellation_token=cancellation_token,
                ))

            with (
                patch("lib.beets_db.BeetsDB", lambda *_args, **_kwargs: beets),
                db._pin_owner_session(
                    importer_token := CancellationToken(),
                ) as importer_owner_session,
            ):
                terminal_job = process_claimed_job(
                    db,
                    claimed_importer,
                    execute_fn=execute_owned_automation,
                    execution_lease=importer_lease,
                    cancellation_token=importer_token,
                    owner_session_identity=importer_owner_session,
                )
            assert terminal_job is not None
            self.assertEqual(len(dispatched_requests), 1)
            self.assertEqual(dispatched_requests[0].execution_lease, importer_lease)
            logs = db.get_log(limit=100)
            outcomes = [str(log["outcome"]) for log in logs]
            request = db.get_request(request_id)
            assert request is not None
            terminal_result = terminal_job.result or {}
            assert_iron_and_wine_outer_policy(
                analyzer_roles=analyzer_roles,
                persisted_grade=(
                    preview_import.spectral.existing.grade
                    if preview_import.spectral.existing is not None else None
                ),
                persisted_generation=(
                    preview_import.spectral.existing.spectral_measurement_version
                    if preview_import.spectral.existing is not None else None
                ),
                dispatch_code=terminal_result.get("code"),
                request_status=str(request["status"]),
                outcomes=outcomes,
                canonical_source_removed=not os.path.exists(candidate),
                cleanup_receipt_recorded=(
                    terminal_result.get("processing_cleanup") is not None
                ),
                job_terminal=(
                    terminal_job.status == "failed" and terminal_job.completed_at is not None
                ),
                owner_cleared=(
                    request.get("active_automation_import_job_id") is None
                ),
            )
            self.assertIn(
                "suspect_lossless_downgrade",
                str(terminal_job.message),
            )
