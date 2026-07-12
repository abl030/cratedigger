"""Generated invariant for independent two-sided spectral attempt audit."""

import os
import tempfile
import unittest
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import patch

from hypothesis import given, strategies as st

from tests.fakes import FakeBeetsDB


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


class TestAttemptAuditGenerated(unittest.TestCase):
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
        from lib.dispatch.core import _attach_attempt_spectral_audit
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
        attached = _attach_attempt_spectral_audit(result, audit)
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
