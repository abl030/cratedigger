"""Generated invariant for independent two-sided spectral attempt audit."""

import configparser
from contextlib import contextmanager
import logging
import os
from pathlib import Path
import subprocess as sp
import tempfile
import unittest
from types import SimpleNamespace
from typing import Any, Literal
from unittest.mock import patch

import msgspec

from hypothesis import example, given, strategies as st
from tests.beets_world import BeetsWorld

import tests._hypothesis_profiles  # noqa: F401  (loads active profile)


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
) -> bool:
    expected = [] if candidate_reused else ["candidate"]
    if not preserve_existing_source:
        expected.append("existing")
    return analyzer_calls == expected


def _run_have_boundary_through_both_adapters(
    *,
    converted_from: str | None,
    lossless_v0_lineage: bool,
    persisted_grade: str,
    persisted_bitrate: int | None,
    scanned_grade: str,
    scanned_bitrate: int | None,
):
    """Drive normal measurement and reused front-gate through one boundary."""
    from lib.beets_db import AlbumInfo
    from lib.config import CratediggerConfig
    from lib.import_queue import (
        IMPORT_JOB_FORCE,
        force_import_dedupe_key,
        force_import_payload,
    )
    from lib.import_preview import preserve_existing_source_spectral
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
    from scripts.import_preview_worker import process_claimed_preview_job
    from tests.fakes import FakeBeetsDB, FakePipelineDB
    from tests.helpers import make_album_quality_evidence, make_request_row

    request_id = 42
    mbid = "mbid-42"
    carries_lossless_lineage = (
        (converted_from or "").lower() in {"flac", "alac", "wav"}
        or lossless_v0_lineage
    )
    current_measurement = AudioQualityMeasurement(
        min_bitrate_kbps=320,
        avg_bitrate_kbps=320,
        median_bitrate_kbps=320,
        format="MP3",
        spectral_grade=persisted_grade,
        spectral_bitrate_kbps=persisted_bitrate,
        was_converted_from=converted_from,
        spectral_subject=(
            "source" if carries_lossless_lineage else "installed"
        ),
        spectral_provenance=(
            "carried" if carries_lossless_lineage else "measured"
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

    with tempfile.TemporaryDirectory(dir=os.getcwd()) as root, \
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
        os.makedirs(os.path.join(processing_dir, "preview"), mode=0o700)
        cfg = CratediggerConfig(
            audio_check_mode="off",
            beets_staging_dir=staging_dir,
            slskd_download_dir=slskd_dir,
            processing_dir=processing_dir,
        )
        Path(candidate, "01.mp3").write_bytes(b"candidate")
        Path(existing, "01.mp3").write_bytes(b"existing")
        current_evidence = make_album_quality_evidence(
            mb_release_id=mbid,
            source_path=existing,
            files=snapshot_audio_files(existing),
            measurement=current_measurement,
            v0_metric=current_v0_metric,
        )
        preserve_source = preserve_existing_source_spectral(current_evidence)
        persisted = SpectralAnalysisDetail(
            attempted=True,
            grade=persisted_grade,
            bitrate_kbps=persisted_bitrate,
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
            format="MP3",
        ))

        normal_calls: list[str] = []
        reused_calls: list[str] = []

        def analyzer_for(calls: list[str]):
            def analyze(path: str) -> SpectralAnalysisDetail:
                role = "existing" if path == existing else "candidate"
                calls.append(role)
                return SpectralAnalysisDetail(
                    attempted=True,
                    grade=scanned_grade if role == "existing" else "genuine",
                    bitrate_kbps=(
                        scanned_bitrate if role == "existing" else None
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
                download_filetype="mp3",
                download_min_bitrate_bps=219_000,
                download_is_vbr=False,
                cfg=cfg,
                existing_spectral_evidence=persisted,
                preserve_existing_source_spectral=preserve_source,
                precomputed_inspection=LocalFileInspection(
                    filetype="mp3",
                    min_bitrate_bps=219_000,
                    is_vbr=False,
                ),
                spectral_detail_analyzer=analyzer_for(normal_calls),
                existing_spectral_resolver=resolve_existing,
            )

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=request_id, mb_release_id=mbid))
        db.upsert_album_quality_evidence(current_evidence)
        stored_current = db.find_album_quality_evidence(
            mb_release_id=mbid,
            snapshot_fingerprint=current_evidence.snapshot_fingerprint,
        )
        assert stored_current is not None and stored_current.id is not None
        db.set_request_current_evidence(request_id, stored_current.id)
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
        candidate_evidence = make_album_quality_evidence(
            mb_release_id=mbid,
            source_path=candidate,
            files=snapshot_audio_files(candidate),
        )
        db.upsert_album_quality_evidence(candidate_evidence)
        stored_candidate = db.find_album_quality_evidence(
            mb_release_id=mbid,
            snapshot_fingerprint=candidate_evidence.snapshot_fingerprint,
        )
        assert stored_candidate is not None and stored_candidate.id is not None
        db.set_download_log_candidate_evidence(
            download_log_id,
            stored_candidate.id,
        )
        claimed = db.claim_next_import_preview_job(worker_id="generated")
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
        normal_calls,
        reused_calls,
        measured.spectral_audit,
        reused,
    )


PreviewJobMode = Literal["automation", "force"]


def assert_candidate_snapshot_reuse(
    *,
    snapshot_changed: bool,
    has_have: bool,
    full_preview_calls: int,
    analyzer_roles: list[str],
    candidate_status: str | None,
    persisted_candidate_grade: str | None,
    expected_candidate_grade: str,
) -> None:
    """Candidate work is once per snapshot; HAVE authority stays separate."""

    if snapshot_changed:
        if full_preview_calls != 1:
            raise AssertionError("changed candidate snapshot did not remeasure")
        if candidate_status == "reused":
            raise AssertionError("changed candidate snapshot was marked reused")
        return

    if full_preview_calls:
        raise AssertionError("matching candidate snapshot ran full preview")
    if "candidate" in analyzer_roles:
        raise AssertionError("matching candidate evidence was analyzed again")
    expected_roles = ["existing"] if has_have else []
    if analyzer_roles != expected_roles:
        raise AssertionError(
            "candidate reuse changed the independent HAVE scan boundary"
        )
    if candidate_status != "reused":
        raise AssertionError("matching candidate snapshot lost reuse provenance")
    if persisted_candidate_grade != expected_candidate_grade:
        raise AssertionError("reused preview dropped persisted candidate spectral")


def _run_candidate_snapshot_reuse_world(
    *,
    job_mode: PreviewJobMode,
    snapshot_changed: bool,
    has_have: bool,
    candidate_grade: str,
    track_count: int,
) -> tuple[int, list[str], str | None, str | None]:
    from lib.config import CratediggerConfig
    from lib.import_queue import (
        IMPORT_JOB_AUTOMATION,
        IMPORT_JOB_FORCE,
        automation_import_dedupe_key,
        force_import_dedupe_key,
        force_import_payload,
    )
    from lib.import_preview import ImportPreviewResult
    from lib.measurement import ExistingSpectralAuditLookup
    from lib.quality import (
        AudioQualityMeasurement,
        ImportResult,
        SpectralAnalysisDetail,
    )
    from lib.quality_evidence import EvidenceBuildResult, snapshot_audio_files
    from scripts.import_preview_worker import process_claimed_preview_job
    from tests.fakes import FakePipelineDB
    from tests.helpers import make_album_quality_evidence, make_request_row

    request_id = 8883
    mbid = "generated-candidate-reuse-mbid"
    with tempfile.TemporaryDirectory(dir=os.getcwd()) as root, \
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
        os.makedirs(os.path.join(processing_dir, "preview"), mode=0o700)
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
                    avg_bitrate_kbps=196,
                    median_bitrate_kbps=195,
                    format="MP3",
                    spectral_grade="genuine",
                    spectral_subject="installed",
                    spectral_provenance="measured",
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
            job = db.enqueue_import_job(
                IMPORT_JOB_AUTOMATION,
                request_id=request_id,
                dedupe_key=automation_import_dedupe_key(request_id),
                payload={},
            )

        candidate_evidence = make_album_quality_evidence(
            mb_release_id=mbid,
            source_path=candidate,
            files=snapshot_audio_files(candidate),
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=245,
                avg_bitrate_kbps=256,
                median_bitrate_kbps=252,
                format="MP3",
                spectral_grade=candidate_grade,
                spectral_subject="source",
                spectral_provenance="measured",
            ),
        )
        db.upsert_album_quality_evidence(candidate_evidence)
        stored_candidate = db.find_album_quality_evidence(
            mb_release_id=mbid,
            snapshot_fingerprint=candidate_evidence.snapshot_fingerprint,
        )
        assert stored_candidate is not None and stored_candidate.id is not None
        if download_log_id is not None:
            db.set_download_log_candidate_evidence(
                download_log_id,
                stored_candidate.id,
            )
        else:
            db.set_import_job_candidate_evidence(job.id, stored_candidate.id)

        if snapshot_changed:
            Path(candidate, f"{track_count:02d}.mp3").write_bytes(
                b"changed-candidate-snapshot"
            )

        claimed = db.claim_next_import_preview_job(worker_id="generated")
        assert claimed is not None and claimed.id == job.id
        full_preview_calls = 0
        analyzer_roles: list[str] = []

        def analyze(path: str):
            role = "existing" if path == existing else "candidate"
            analyzer_roles.append(role)
            return SpectralAnalysisDetail(
                attempted=True,
                grade="suspect" if role == "existing" else candidate_grade,
                bitrate_kbps=128 if role == "existing" else None,
            )

        def full_preview(db_arg: Any, _job: Any) -> ImportPreviewResult:
            nonlocal full_preview_calls
            full_preview_calls += 1
            fresh = make_album_quality_evidence(
                mb_release_id=mbid,
                source_path=candidate,
                files=snapshot_audio_files(candidate),
                measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=245,
                    avg_bitrate_kbps=256,
                    median_bitrate_kbps=252,
                    format="MP3",
                    spectral_grade=candidate_grade,
                    spectral_subject="source",
                    spectral_provenance="measured",
                ),
            )
            db_arg.upsert_album_quality_evidence(fresh)
            persisted = db_arg.find_album_quality_evidence(
                mb_release_id=mbid,
                snapshot_fingerprint=fresh.snapshot_fingerprint,
            )
            assert persisted is not None and persisted.id is not None
            if download_log_id is not None:
                db_arg.set_download_log_candidate_evidence(
                    download_log_id,
                    persisted.id,
                )
            else:
                db_arg.set_import_job_candidate_evidence(job.id, persisted.id)
            return ImportPreviewResult(
                mode="path",
                verdict="evidence_ready",
                decision="import",
                reason="import",
                source_path=candidate,
            )

        def load_current(*_args: Any, **_kwargs: Any) -> EvidenceBuildResult:
            if current is None:
                return EvidenceBuildResult(
                    None,
                    "empty_current",
                    "exact album not in beets",
                )
            return EvidenceBuildResult(current, "ready")

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
            current_evidence_loader=load_current,
            runtime_config=cfg,
        )
        assert updated is not None
        preview_result = updated.preview_result or {}
        candidate_status = preview_result.get("candidate_status")
        persisted_grade = None
        import_result_raw = preview_result.get("import_result")
        if isinstance(import_result_raw, dict):
            import_result = ImportResult.from_dict(import_result_raw)
            if import_result.spectral.candidate is not None:
                persisted_grade = import_result.spectral.candidate.grade
        return (
            full_preview_calls,
            analyzer_roles,
            candidate_status if isinstance(candidate_status, str) else None,
            persisted_grade,
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
    from lib.quality import DownloadInfo, ImportResult, QualityComparisonBasis
    from tests.fakes import FakePipelineDB
    from tests.helpers import (
        finalize_claimed_dispatch,
        make_album_quality_evidence,
        make_import_result,
        make_request_row,
        noop_quality_gate,
        patch_dispatch_externals,
    )

    db = FakePipelineDB()
    db.seed_request(make_request_row(
        id=42,
        mb_release_id="generated-mbid",
        status="downloading",
        search_filetype_override="mp3",
        active_download_state={"files": [], "filetype": "mp3"},
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
        del args, kwargs
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

        db.set_tracks(42, [{"track_number": 1, "title": "One"}])
        with tempfile.TemporaryDirectory() as source:
            for filename in ("01.mp3", "bonus.mp3"):
                with open(os.path.join(source, filename), "wb") as handle:
                    handle.write(b"audio")
            job = db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                payload={"failed_path": source},
            )
            preview_result: dict[str, Any] = {}
            if audit is not None:
                builtins = msgspec.to_builtins(ImportResult(spectral=audit))
                assert isinstance(builtins, dict)
                preview_result["import_result"] = builtins
            db.mark_import_job_preview_importable(
                job.id,
                preview_result=preview_result,
            )
            with _silence_logs():
                dispatch_import_from_db(
                    db,  # type: ignore[arg-type]
                    request_id=42,
                    failed_path=source,
                    import_job_id=job.id,
                    source_username="generated-user",
                    beets_library_db_path=str(beets.library_db),
                    beets_library_root=str(beets.library_root),
                )
    else:
        from lib.import_queue import IMPORT_JOB_AUTOMATION
        from lib.quality_evidence import snapshot_audio_files

        with tempfile.TemporaryDirectory() as source:
            with open(os.path.join(source, "01.mp3"), "wb") as handle:
                handle.write(b"audio")
            db.request(42)["active_download_state"]["current_path"] = source
            job = db.enqueue_import_job(
                IMPORT_JOB_AUTOMATION,
                request_id=42,
                payload={},
            )
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
                preview_result={"ready": True},
            )
            claimed = db.claim_next_import_job(worker_id="generated-importer")
            assert claimed is not None and claimed.id == job.id
            with patch_dispatch_externals(), _silence_logs():
                outcome = dispatch_import_core(
                    path=source,
                    mb_release_id="generated-mbid",
                    request_id=42,
                    label="Generated Artist - Generated Album",
                    beets_harness_path=cfg.beets_harness_path,
                    db=db,  # type: ignore[arg-type]
                    dl_info=DownloadInfo(
                        username="generated-user", filetype="mp3"
                    ),
                    cfg=cfg,
                    attempt_spectral_audit=audit,
                    run_import_fn=run_import,
                    quality_gate_fn=quality_gate,
                    candidate_import_job_id=claimed.id,
                    beets_library_db_path=str(beets.library_db),
                    beets_library_root=str(beets.library_root),
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
        "job_status": final_job.status,
        "denylist": [(row.username, row.reason) for row in db.denylist],
    }


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
        ))

    def test_have_boundary_checker_rejects_blanket_scan_mutant(self):
        self.assertFalse(_have_scan_boundary_holds(
            ["candidate", "existing"],
            preserve_existing_source=True,
            candidate_reused=True,
        ))

    def test_have_boundary_checker_rejects_reused_candidate_rescan(self):
        self.assertFalse(_have_scan_boundary_holds(
            ["candidate", "existing"],
            preserve_existing_source=False,
            candidate_reused=True,
        ))

    def test_candidate_reuse_checker_rejects_matching_snapshot_rescan(self):
        with self.assertRaises(AssertionError):
            assert_candidate_snapshot_reuse(
                snapshot_changed=False,
                has_have=False,
                full_preview_calls=0,
                analyzer_roles=["candidate"],
                candidate_status="reused",
                persisted_candidate_grade="genuine",
                expected_candidate_grade="genuine",
            )

    def test_candidate_reuse_checker_rejects_changed_snapshot_skip(self):
        with self.assertRaises(AssertionError):
            assert_candidate_snapshot_reuse(
                snapshot_changed=True,
                has_have=False,
                full_preview_calls=0,
                analyzer_roles=[],
                candidate_status="reused",
                persisted_candidate_grade="genuine",
                expected_candidate_grade="genuine",
            )


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
        has_have=False,
        candidate_grade="genuine",
        track_count=12,
    )
    @example(
        job_mode="automation",
        snapshot_changed=False,
        has_have=True,
        candidate_grade="suspect",
        track_count=1,
    )
    def test_candidate_measurement_is_once_per_snapshot_across_job_modes(
        self,
        job_mode: PreviewJobMode,
        snapshot_changed: bool,
        has_have: bool,
        candidate_grade: str,
        track_count: int,
    ):
        (
            full_preview_calls,
            analyzer_roles,
            candidate_status,
            persisted_candidate_grade,
        ) = _run_candidate_snapshot_reuse_world(
            job_mode=job_mode,
            snapshot_changed=snapshot_changed,
            has_have=has_have,
            candidate_grade=candidate_grade,
            track_count=track_count,
        )
        assert_candidate_snapshot_reuse(
            snapshot_changed=snapshot_changed,
            has_have=has_have,
            full_preview_calls=full_preview_calls,
            analyzer_roles=analyzer_roles,
            candidate_status=candidate_status,
            persisted_candidate_grade=persisted_candidate_grade,
            expected_candidate_grade=candidate_grade,
        )

    def test_gespenst_mp3_scans_exact_have_through_both_adapters(self):
        (
            preserve_source,
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
        )

        self.assertFalse(preserve_source)
        self.assertEqual(normal_calls, ["candidate", "existing"])
        self.assertEqual(reused_calls, ["existing"])
        for audit in (normal_audit, reused_audit):
            assert audit.existing is not None
            self.assertEqual(audit.existing.grade, "suspect")
            self.assertEqual(audit.existing.bitrate_kbps, 128)

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
        from lib.import_preview import load_persisted_existing_spectral
        from lib.quality import AudioQualityMeasurement
        from tests.fakes import FakePipelineDB
        from tests.helpers import make_album_quality_evidence, make_request_row

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

        _, detail, authoritative = load_persisted_existing_spectral(db, 42)

        self.assertTrue(authoritative)
        self.assertTrue(_authoritative_have_matches(
            detail,
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
        )
        # R19: a source-subject V0 anchor alone proves lossless lineage —
        # enrichment-born rows carry no was_converted_from, and their
        # installed derivative must not be scanned into a fresh grade.
        self.assertEqual(
            preserve_existing_source,
            (
                (converted_from or "").lower() in LOSSLESS_CODECS
                or lossless_v0_lineage
            ),
        )
        self.assertTrue(_have_scan_boundary_holds(
            normal_calls,
            preserve_existing_source=preserve_existing_source,
            candidate_reused=False,
        ))
        self.assertTrue(_have_scan_boundary_holds(
            reused_calls,
            preserve_existing_source=preserve_existing_source,
            candidate_reused=True,
        ))
        for audit in (normal_audit, reused_audit):
            assert audit.existing is not None
            if preserve_existing_source:
                self.assertEqual(audit.existing.grade, persisted_grade)
                self.assertEqual(audit.existing.bitrate_kbps, persisted_bitrate)
            else:
                self.assertEqual(audit.existing.grade, scanned_grade)
                self.assertEqual(audit.existing.bitrate_kbps, scanned_bitrate)

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
            self.assertIsNone(audited["import_result"])
            self.assertIsNone(unaudited["import_result"])
            self.assertEqual(audited["job_status"], "recovery_required")
            self.assertEqual(unaudited["job_status"], "recovery_required")
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
            AudioQualityMeasurement, ImportResult, SpectralAnalysisDetail,
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
            AudioQualityMeasurement, ImportResult, SpectralAnalysisDetail,
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
