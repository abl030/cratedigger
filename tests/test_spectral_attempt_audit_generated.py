"""Generated invariant for independent two-sided spectral attempt audit."""

from contextlib import contextmanager
import logging
import os
import subprocess as sp
import tempfile
import unittest
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import patch

import msgspec

from hypothesis import given, strategies as st

import tests._hypothesis_profiles  # noqa: F401  (loads active profile)
from tests.fakes import FakeBeetsDB


@contextmanager
def _silence_logs():
    previous_level = logging.root.manager.disable
    logging.disable(logging.CRITICAL)
    try:
        yield
    finally:
        logging.disable(previous_level)


def _both_sides_attempted(calls: list[str]) -> bool:
    return calls.count("candidate") == 1 and calls.count("existing") == 1


def _policy_snapshot(result):
    return (result.decision, result.new_measurement, result.existing_measurement)


def _policy_snapshot_unchanged(before, after) -> bool:
    return _policy_snapshot(after) == before


def _relative_path_resolved(path: str | None, expected: str) -> bool:
    return path is not None and path == expected and os.path.isabs(path)


def _audit_only_policy_inputs_unchanged(
    inputs: tuple[str | None, int | None],
) -> bool:
    return inputs == (None, None)


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
    def test_checker_rejects_short_circuiting_known_bad_trace(self):
        self.assertFalse(_both_sides_attempted(["candidate"]))

    def test_relative_path_checker_rejects_unresolved_known_bad_path(self):
        self.assertFalse(
            _relative_path_resolved("Artist/Album", "/library/Artist/Album")
        )

    def test_audit_only_policy_checker_rejects_known_bad_inputs(self):
        self.assertFalse(
            _audit_only_policy_inputs_unchanged(("suspect", 96))
        )

    def test_finalization_checker_rejects_skipped_terminal_finalization(self):
        from lib.quality import ImportResult, SpectralAnalysisDetail, SpectralDetail

        audit = SpectralDetail(candidate=SpectralAnalysisDetail(
            attempted=True, grade="suspect", bitrate_kbps=96))
        skipped_finalization = ImportResult(decision="import").to_json()
        self.assertFalse(_persisted_attempt_has_exact_audit(
            skipped_finalization, audit))


class TestAttemptAuditGenerated(unittest.TestCase):
    @given(
        mode=st.sampled_from((
            "success", "rejection", "no_json", "timeout",
            "pre_result_exception", "post_result_exception",
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
        artist=st.text(
            alphabet=st.characters(whitelist_categories=("Ll", "Lu", "Nd")),
            min_size=1,
            max_size=12,
        ),
        album=st.text(
            alphabet=st.characters(whitelist_categories=("Ll", "Lu", "Nd")),
            min_size=1,
            max_size=12,
        ),
    )
    def test_relative_existing_path_resolves_under_explicit_library_root(
        self, artist: str, album: str,
    ):
        from harness import import_one

        relative_path = os.path.join(artist, album)

        class RelativePathBeets(FakeBeetsDB):
            def get_album_path(self, mb_release_id: str) -> str | None:
                return relative_path

        with tempfile.TemporaryDirectory() as library_root:
            expected = os.path.join(library_root, relative_path)
            os.makedirs(expected)
            resolution = import_one._resolve_existing_spectral_path(
                cast(Any, RelativePathBeets()),
                "mbid-generated",
                True,
                beets_library_root=library_root,
            )

            self.assertTrue(
                _relative_path_resolved(resolution.audit_path, expected)
            )
            self.assertFalse(resolution.legacy_policy_path_usable)
            self.assertIsNone(resolution.failure)

    @given(
        grade=st.sampled_from(["genuine", "suspect", "likely_transcode"]),
        bitrate=st.one_of(st.none(), st.integers(min_value=32, max_value=400)),
        candidate_spectral_ok=st.booleans(),
    )
    def test_root_resolved_audit_never_becomes_legacy_policy_input(
        self,
        grade: str,
        bitrate: int | None,
        candidate_spectral_ok: bool,
    ):
        from harness import import_one
        from lib.quality import SpectralAnalysisDetail

        resolution = import_one.ExistingSpectralPathResolution(
            audit_path="/library/Artist/Album",
            legacy_policy_path_usable=False,
        )
        audit = SpectralAnalysisDetail(
            attempted=True,
            grade=grade,
            bitrate_kbps=bitrate,
        )

        inputs = import_one._existing_spectral_policy_inputs(
            resolution,
            audit,
            candidate_spectral_ok=candidate_spectral_ok,
        )

        self.assertTrue(_audit_only_policy_inputs_unchanged(inputs))

    @given(
        candidate_fails=st.booleans(),
        existing_fails=st.booleans(),
    )
    def test_each_available_side_is_attempted_despite_other_failure(
        self, candidate_fails: bool, existing_fails: bool,
    ):
        from lib.measurement import collect_attempt_spectral_audit

        calls: list[str] = []

        def analyze(path: str, trim_seconds: int = 30):
            side = path.removeprefix("/")
            calls.append(side)
            if (side == "candidate" and candidate_fails) or (
                side == "existing" and existing_fails
            ):
                raise RuntimeError(f"{side} failed")
            return SimpleNamespace(
                grade="genuine", estimated_bitrate_kbps=None,
                suspect_pct=0.0, tracks=[],
            )

        with patch("lib.measurement.spectral_analyze", side_effect=analyze):
            audit = collect_attempt_spectral_audit("/candidate", "/existing")

        assert audit.candidate is not None
        assert audit.existing is not None
        self.assertTrue(_both_sides_attempted(calls))
        self.assertEqual(audit.candidate.error is not None, candidate_fails)
        self.assertEqual(audit.existing.error is not None, existing_fails)

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
