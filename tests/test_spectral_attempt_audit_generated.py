"""Generated invariant for independent two-sided spectral attempt audit."""

from contextlib import contextmanager
import logging
import os
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
    return (result.decision, result.new_measurement, result.existing_measurement)


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
        make_import_result,
        make_request_row,
        noop_quality_gate,
        patch_dispatch_externals,
    )

    db = FakePipelineDB()
    db.seed_request(make_request_row(
        id=42,
        status="downloading",
        search_filetype_override="mp3",
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
        from lib.import_queue import IMPORT_JOB_MANUAL, manual_import_payload

        db.set_tracks(42, [{"track_number": 1, "title": "One"}])
        with tempfile.TemporaryDirectory() as source:
            for filename in ("01.mp3", "bonus.mp3"):
                with open(os.path.join(source, filename), "wb") as handle:
                    handle.write(b"audio")
            job = db.enqueue_import_job(
                IMPORT_JOB_MANUAL,
                request_id=42,
                payload=manual_import_payload(failed_path=source),
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
        with patch_dispatch_externals(), _silence_logs():
            dispatch_import_core(
                path="/tmp/cratedigger-generated-attempt",
                mb_release_id="generated-mbid",
                request_id=42,
                label="Generated Artist - Generated Album",
                beets_harness_path=cfg.beets_harness_path,
                db=db,  # type: ignore[arg-type]
                dl_info=DownloadInfo(username="generated-user", filetype="mp3"),
                cfg=cfg,
                attempt_spectral_audit=audit,
                run_import_fn=run_import,
                quality_gate_fn=quality_gate,
            )

    last_log = db.download_logs[-1]
    return {
        "import_result": last_log.import_result,
        "outcomes": [row.outcome for row in db.download_logs],
        "status": db.request(42)["status"],
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


class TestAttemptAuditGenerated(unittest.TestCase):
    @given(
        persisted_grade=st.sampled_from((
            "genuine", "suspect", "likely_transcode",
        )),
        persisted_bitrate=st.one_of(
            st.none(), st.integers(min_value=32, max_value=400),
        ),
        derivative_grade=st.sampled_from((
            "genuine", "suspect", "likely_transcode",
        )),
        derivative_bitrate=st.one_of(
            st.none(), st.integers(min_value=32, max_value=400),
        ),
    )
    def test_have_always_uses_persisted_source_not_derivative_analysis(
        self,
        persisted_grade: str,
        persisted_bitrate: int | None,
        derivative_grade: str,
        derivative_bitrate: int | None,
    ):
        from lib.measurement import collect_attempt_spectral_audit
        from lib.quality import SpectralAnalysisDetail

        persisted = SpectralAnalysisDetail(
            attempted=True,
            grade=persisted_grade,
            bitrate_kbps=persisted_bitrate,
        )
        calls: list[object] = []

        def analyze(path: object, trim_seconds: int = 30):
            del trim_seconds
            calls.append(path)
            return SimpleNamespace(
                grade=(
                    "genuine" if path == "candidate" else derivative_grade
                ),
                estimated_bitrate_kbps=(
                    None if path == "candidate" else derivative_bitrate
                ),
                suspect_pct=0.0,
                tracks=[],
            )

        with patch("lib.measurement.spectral_analyze", side_effect=analyze):
            audit = collect_attempt_spectral_audit("candidate", persisted)

        self.assertTrue(_have_preserves_persisted_source(
            audit,
            expected_grade=persisted_grade,
            expected_bitrate=persisted_bitrate,
            analyzer_calls=calls,
        ))

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

        self.assertTrue(_persisted_attempt_has_exact_audit(
            audited["import_result"], audit))
        self.assertEqual(
            _policy_payload(audited["import_result"]),
            _policy_payload(unaudited["import_result"]),
        )
        self.assertEqual(audited["outcomes"], unaudited["outcomes"])
        self.assertEqual(audited["status"], unaudited["status"])
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
            new_measurement=AudioQualityMeasurement(
                min_bitrate_kbps=new_bitrate, avg_bitrate_kbps=new_bitrate,
                median_bitrate_kbps=new_bitrate, format="MP3"),
            existing_measurement=AudioQualityMeasurement(
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
            new_measurement=AudioQualityMeasurement(
                min_bitrate_kbps=320, avg_bitrate_kbps=320,
                median_bitrate_kbps=320, format="MP3"),
        )
        audit = SpectralDetail(candidate=SpectralAnalysisDetail(
            attempted=True, grade="likely_transcode", bitrate_kbps=96))
        before = _policy_snapshot(result)
        mutant = ImportResult(
            decision=result.decision,
            new_measurement=AudioQualityMeasurement(
                min_bitrate_kbps=96, avg_bitrate_kbps=96,
                median_bitrate_kbps=96, format="MP3",
                spectral_grade=audit.candidate.grade if audit.candidate else None,
            ),
            spectral=audit,
        )
        self.assertFalse(_policy_snapshot_unchanged(before, mutant))
