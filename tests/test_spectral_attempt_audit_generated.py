"""Generated invariant for independent two-sided spectral attempt audit."""

from contextlib import contextmanager
import logging
import os
from pathlib import Path
import subprocess as sp
import tempfile
import unittest
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

import msgspec

from hypothesis import given, strategies as st

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
) -> bool:
    expected = ["candidate"] if preserve_existing_source else [
        "candidate", "existing",
    ]
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
    cfg = CratediggerConfig(audio_check_mode="off")
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

    with tempfile.TemporaryDirectory() as candidate, \
         tempfile.TemporaryDirectory() as existing:
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
        download_log_id = db.log_download(request_id, outcome="rejected")
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
        ))

    def test_have_boundary_checker_rejects_blanket_scan_mutant(self):
        self.assertFalse(_have_scan_boundary_holds(
            ["candidate", "existing"],
            preserve_existing_source=True,
        ))


class TestAttemptAuditGenerated(unittest.TestCase):
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
        self.assertEqual(reused_calls, ["candidate", "existing"])
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
        for calls in (normal_calls, reused_calls):
            self.assertTrue(_have_scan_boundary_holds(
                calls,
                preserve_existing_source=preserve_existing_source,
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
        )
        unaudited = _run_dispatch_finalization_world(
            mode=mode,
            audit=None,
            new_bitrate=new_bitrate,
            existing_bitrate=existing_bitrate,
            converted=converted,
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
