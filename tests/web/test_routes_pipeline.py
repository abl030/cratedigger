"""Contract tests for web/routes/pipeline.py — core pipeline CRUD, log,
detail, recent, all, search, downloading, import jobs, and wrong-match
triage sweep.

Split from tests/test_web_server.py (#408). Shared harness in
tests/web/_harness.py. Triage and long-tail contract tests moved to
tests/web/test_routes_triage.py / tests/web/test_routes_long_tail.py
(#481 item 3). Dashboard, Decisions (constants/simulate), and
beets-distance contract tests moved to
tests/web/test_routes_pipeline_dashboard.py,
tests/web/test_routes_decisions.py, and
tests/web/test_routes_beets_distance.py (#522), following
web/routes/pipeline.py's own split.
"""
import os
import sys
import threading
import unittest
from datetime import UTC, datetime
from typing import ClassVar
from unittest.mock import ANY, patch

import msgspec

from web.classify import ClassifiedEntry, LogEntry, classify_log_entry

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from lib.quality import (
    CandidateSummary,
    HarnessItem,
    HarnessTrackInfo,
    TrackMapping,
    ValidationResult,
)
from tests.fakes import FakeBeetsDB
from tests.helpers import (
    handoff_automation_owner,
    make_request_row,
)
from tests.web._harness import (
    _assert_required_fields,
    _FakeDbWebServerCase,
    _fresh_triage_runner,
)


def _validation_result_blob(
    mappings: list[TrackMapping],
    *,
    distance: float = 0.05,
    mbid: str = "rel-track-length-warning",
    scenario: str = "strong_match",
    valid: bool = True,
) -> dict[str, object]:
    """A ``validation_result`` blob shaped exactly like
    ``ValidationResult.to_json()`` writes it — built from the real Structs
    (``.claude/rules/test-fidelity.md`` Rule C), never hand-typed keys."""
    candidate = CandidateSummary(
        mbid=mbid, distance=distance, is_target=True, mapping=mappings,
    )
    result = ValidationResult(
        valid=valid, scenario=scenario, distance=distance,
        mbid_found=True, target_mbid=mbid, candidates=[candidate],
    )
    return msgspec.to_builtins(result)


class TestPipelineRouteContracts(_FakeDbWebServerCase):
    """Contract tests for frontend-consumed pipeline GET routes."""

    PIPELINE_ITEM_REQUIRED_FIELDS: ClassVar = {
        "id", "artist_name", "album_title", "year", "format", "country",
        "source", "created_at", "status", "search_attempts",
        "download_attempts", "validation_attempts", "beets_distance",
        "mb_release_id",
        # Release-group id surfaces so the pipeline-row Replace button
        # (R7) can render — both the standard-mode source label and
        # the picker's inverted-row sibling lookup need it.
        "mb_release_group_id",
        "current_spectral_bitrate",
        "last_download_spectral_bitrate", "current_spectral_grade",
        "last_download_spectral_grade", "verified_lossless",
        "processing_owner",
    }
    LOG_ENTRY_REQUIRED_FIELDS = {
        "id", "request_id", "outcome", "album_title", "artist_name",
        "created_at", "badge", "badge_class", "border_color", "summary",
        "verdict", "in_beets",
        # The evidence strip's codec prefix (issue #575 PR2) — classifier
        # field the raw LogEntry columns don't carry; must be forwarded.
        "downloaded_label",
        # The on-disk codec at download time (from import_result JSONB) —
        # rank-driven upgrades at equal bitrate are unreadable without it.
        "existing_format",
        # The persisted QualityComparisonBasis (JSON-plain dict, null on
        # legacy rows) — the decision's own comparison for the evidence
        # strip; request 6039 tautology fix.
        "comparison_basis",
        # Issue #130: post-import `beet move` failures surface as typed
        # reason + detail so the frontend can render a warning chip.
        # Null on clean rows; the field must always be present.
        "disambiguation_failure", "disambiguation_detail",
        # Postflight bad-extension detection is warning-only but must be
        # surfaced in Recents so it is not buried in JSONB.
        "bad_extensions",
        # Wrong-match triage audit is display-only history metadata; clean
        # rows emit null/empty values so the frontend can render conditionally.
        "wrong_match_triage_action", "wrong_match_triage_summary",
        "wrong_match_triage_reason", "wrong_match_triage_preview_verdict",
        "wrong_match_triage_preview_decision",
        "wrong_match_triage_stage_chain", "wrong_match_triage_detail",
    } | {field.name for field in msgspec.structs.fields(ClassifiedEntry)}
    HISTORY_REQUIRED_FIELDS = {
        "id", "request_id", "outcome", "created_at", "soulseek_username",
        "badge", "badge_class", "border_color",
        "downloaded_label", "verdict", "beets_scenario", "beets_distance",
        "apply_beets_distance",
        "disambiguation_failure", "disambiguation_detail", "bad_extensions",
        "spectral_grade", "spectral_bitrate", "existing_min_bitrate",
        "existing_spectral_grade", "existing_spectral_bitrate", "existing_format",
        "source", "youtube_metadata",
        "wrong_match_triage_action", "wrong_match_triage_summary",
        "wrong_match_triage_reason", "wrong_match_triage_preview_verdict",
        "wrong_match_triage_preview_decision",
        "wrong_match_triage_stage_chain", "wrong_match_triage_detail",
    } | {field.name for field in msgspec.structs.fields(ClassifiedEntry)}
    STATUS_WANTED_REQUIRED_FIELDS: ClassVar = {
        "id", "artist", "album", "mb_release_id", "source", "created_at",
    }
    IMPORT_PREVIEW_REQUIRED_FIELDS: ClassVar = {
        "mode", "verdict", "would_import", "confident_reject", "uncertain",
        "cleanup_eligible", "decision", "reason", "stage_chain",
    }
    WRONG_MATCH_TRIAGE_SUMMARY_REQUIRED_FIELDS: ClassVar = {
        "processed", "deleted", "deleted_verified_lossless_parent",
        "kept_would_import", "kept_uncertain",
        "skipped_candidate_evidence_missing", "skipped_candidate_evidence_stale",
        "skipped_current_evidence_missing", "skipped_current_evidence_stale",
        "skipped_current_evidence_failed",
        "skipped_active_job", "skipped_invalid_row", "skipped_missing_path",
        "skipped_operational", "delete_failed", "results",
        # Issue #1083: distinguishes an operator-stopped sweep from a full
        # completion; the deleted/kept/skipped counts above still reflect
        # exactly what ran before the stop either way.
        "cancelled",
    }
    WRONG_MATCH_TRIAGE_STATUS_REQUIRED_FIELDS: ClassVar = {
        "state", "started_at", "finished_at", "summary", "error",
    }
    IMPORT_JOB_REQUIRED_FIELDS: ClassVar = {
        "id", "job_type", "status", "request_id", "dedupe_key", "payload",
        "result", "message", "error", "attempts", "worker_id", "created_at",
        "updated_at", "started_at", "heartbeat_at", "completed_at", "deduped",
        "preview_status", "preview_result", "preview_message", "preview_error",
        "preview_attempts", "preview_worker_id", "preview_started_at",
        "preview_heartbeat_at", "preview_completed_at", "importable_at",
        "candidate_evidence_id", "beets_launch_authorized_at",
        "beets_launch_release_id", "beets_launch_source_path",
        "beets_launch_request_status",
        "beets_launch_snapshot_fingerprint",
    }
    DISK_COVERAGE_COUNT_FIELDS: ClassVar = {
        "active_total", "on_disk_total", "off_disk_total", "by_status",
        "on_disk_by_status", "off_disk_by_status", "inverse_total",
    }
    DISK_COVERAGE_ROW_FIELDS: ClassVar = {
        "id", "status", "artist_name", "album_title", "mb_release_id",
        "discogs_release_id", "resolution",
    }
    DISK_COVERAGE_INVERSE_FIELDS: ClassVar = {
        "id", "album", "albumartist", "mb_albumid", "discogs_albumid",
    }

    def setUp(self) -> None:
        super().setUp()
        # The detail/log fixtures: one imported request with a track and
        # a real success download row, plus one wanted request.
        self.db.seed_request(make_request_row(
            id=100, status="imported", min_bitrate=320,
        ))
        self.db.set_tracks(100, [
            {"disc_number": 1, "track_number": 1, "title": "Track",
             "length_seconds": 180},
        ])
        self.db.log_download(
            100, outcome="success", beets_scenario="strong_match",
            beets_distance=0.012, soulseek_username="testuser",
            filetype="mp3", bitrate=320000, actual_filetype="mp3",
            actual_min_bitrate=320, valid=True,
        )
        self.db.seed_request(make_request_row(
            id=101, status="wanted", source="request",
        ))

    def test_pipeline_log_contract(self):
        status, data = self._get("/api/pipeline/log")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, {"log", "counts"}, "pipeline log response")
        _assert_required_fields(self, data["log"][0], self.LOG_ENTRY_REQUIRED_FIELDS,
                                "pipeline log entry")
        _assert_required_fields(
            self,
            data["counts"],
            {
                "all", "imported", "rejected", "matches_24h",
                "matches_6h", "matches_per_hour_24h",
                "matches_per_hour_6h",
            },
            "pipeline log counts",
        )

    def test_have_analysis_error_contract_and_copy(self):
        """AE3: environment failures stay diagnostic and retryable."""
        installed_path = "/mnt/virtio/Music/Beets/Low/Things We Lost"
        candidate_reference = "/mnt/virtio/Music/Incoming/auto-import/101"
        raw_error = "PermissionError: [Errno 13] Permission denied"
        failure = {
            "failure_category": "permission_denied",
            "error": raw_error,
            "installed_path": installed_path,
            "candidate_reference": candidate_reference,
        }
        log_id = self.db.log_download(
            101,
            outcome="have_analysis_error",
            beets_scenario="have_analysis_error",
            soulseek_username="archive-peer",
            download_path=installed_path,
            staged_path=candidate_reference,
            error_message=raw_error,
            validation_result=failure,
        )

        status, data = self._get("/api/pipeline/log")

        self.assertEqual(status, 200)
        item = next(row for row in data["log"] if row["id"] == log_id)
        _assert_required_fields(
            self,
            item,
            {
                "download_path", "staged_path", "failure_category",
                "analysis_error", "installed_path", "candidate_reference",
            },
            "HAVE analysis error Recents row",
        )
        self.assertEqual(item["badge"], "Environment failure")
        self.assertEqual(item["badge_class"], "badge-warn")
        self.assertEqual(item["border_color"], "#a86f20")
        self.assertEqual(item["failure_category"], "permission_denied")
        self.assertEqual(item["analysis_error"], raw_error)
        self.assertEqual(item["installed_path"], installed_path)
        self.assertEqual(item["candidate_reference"], candidate_reference)
        self.assertEqual(item["request_status"], "wanted")
        self.assertIn("Search remains open", item["summary"])
        self.assertIn("future download will retry", item["summary"])

        detail_status, detail = self._get("/api/pipeline/101")
        self.assertEqual(detail_status, 200)
        history_item = next(
            row for row in detail["history"] if row["id"] == log_id
        )
        self.assertEqual(history_item["failure_category"], "permission_denied")
        self.assertEqual(history_item["installed_path"], installed_path)
        self.assertEqual(history_item["candidate_reference"], candidate_reference)

    def test_have_analysis_error_classification_survives_malformed_payload(self):
        entry = LogEntry.from_row({
            "outcome": "have_analysis_error",
            "download_path": "/library/current",
            "staged_path": "/incoming/candidate",
            "error_message": "analyser exited 9",
            "validation_result": "{malformed",
        })

        classified = classify_log_entry(entry)

        self.assertEqual(classified.badge, "Environment failure")
        self.assertEqual(classified.badge_class, "badge-warn")
        self.assertEqual(classified.border_color, "#a86f20")
        self.assertIsNone(classified.failure_category)
        self.assertEqual(classified.analysis_error, "analyser exited 9")
        self.assertEqual(classified.installed_path, "/library/current")
        self.assertEqual(classified.candidate_reference, "/incoming/candidate")
        self.assertIn("request lifecycle was preserved", classified.verdict)

    # --- Issue #1178 (post-correction): render-time track-length warning ---
    #
    # Every pin here drives the FULL outermost adapter — the actual
    # /api/pipeline/log route, through build_recents_download_log_rows,
    # classify_log_entry, and JSON encoding — not classify_log_entry alone.
    # The prior (reverted) validation-time-gate review found helper-level
    # pins let adapter-wiring mutants survive (F3/M1); this is the fix.

    def test_track_length_warning_surfaces_on_the_1178_world(self):
        """download_log 40061's exact shape: a 237.6s file paired against
        MB track 17's declared 15.0s, at a distance (0.1441) that cleared
        the 0.15 gate legitimately."""
        self.db.seed_request(make_request_row(id=900, status="imported"))
        log_id = self.db.log_download(
            900, outcome="success",
            validation_result=_validation_result_blob(
                distance=0.1441,
                mappings=[
                    TrackMapping(
                        item=HarnessItem(
                            path="02 The Outside.flac", length=237.7),
                        track=HarnessTrackInfo(
                            title="The Outside", length=237.0),
                    ),
                    TrackMapping(
                        item=HarnessItem(
                            path="00 - Hidden Track.flac",
                            length=237.633167),
                        track=HarnessTrackInfo(
                            title="Lost Weekend", length=15.0),
                    ),
                ],
            ),
        )

        status, data = self._get("/api/pipeline/log")

        self.assertEqual(status, 200)
        item = next(row for row in data["log"] if row["id"] == log_id)
        self.assertEqual(
            item["track_length_warning"],
            "Track length contradicts the matched release: "
            "'00 - Hidden Track.flac' is 237.6s where the release "
            "declares 15.0s",
        )

    def test_track_length_warning_skips_a_pair_with_no_declared_length(self):
        """A CD pregap hidden track carries no declared MB length (0.0) —
        the warning must skip it, never flag it, even though the file
        measures long."""
        self.db.seed_request(make_request_row(id=901, status="imported"))
        log_id = self.db.log_download(
            901, outcome="success",
            validation_result=_validation_result_blob(mappings=[
                TrackMapping(
                    item=HarnessItem(
                        path="00 - Hidden Track.flac", length=237.6),
                    track=HarnessTrackInfo(title="Hidden Track", length=0.0),
                ),
                TrackMapping(
                    item=HarnessItem(path="02.flac", length=100.0),
                    track=HarnessTrackInfo(title="Two", length=100.0),
                ),
            ]),
        )

        status, data = self._get("/api/pipeline/log")

        self.assertEqual(status, 200)
        item = next(row for row in data["log"] if row["id"] == log_id)
        self.assertIsNone(item["track_length_warning"])

    def test_track_length_warning_absent_under_the_bound(self):
        self.db.seed_request(make_request_row(id=902, status="imported"))
        log_id = self.db.log_download(
            902, outcome="success",
            validation_result=_validation_result_blob(mappings=[
                TrackMapping(
                    item=HarnessItem(path="01.flac", length=100.0),
                    track=HarnessTrackInfo(title="One", length=105.0),
                ),
            ]),
        )

        status, data = self._get("/api/pipeline/log")

        self.assertEqual(status, 200)
        item = next(row for row in data["log"] if row["id"] == log_id)
        self.assertIsNone(item["track_length_warning"])

    def test_track_length_warning_absent_at_exactly_the_bound(self):
        """The comparison is strictly-greater-than: a deviation exactly AT
        the bound still clears (no warning)."""
        from web.classify import TRACK_LENGTH_WARNING_BOUND_SECONDS

        self.db.seed_request(make_request_row(id=905, status="imported"))
        log_id = self.db.log_download(
            905, outcome="success",
            validation_result=_validation_result_blob(mappings=[
                TrackMapping(
                    item=HarnessItem(
                        path="01.flac",
                        length=100.0 + TRACK_LENGTH_WARNING_BOUND_SECONDS,
                    ),
                    track=HarnessTrackInfo(title="One", length=100.0),
                ),
            ]),
        )

        status, data = self._get("/api/pipeline/log")

        self.assertEqual(status, 200)
        item = next(row for row in data["log"] if row["id"] == log_id)
        self.assertIsNone(item["track_length_warning"])

    def test_track_length_warning_absent_on_a_rejected_row(self):
        """The same 222.6s-shaped mismatch on a REJECTED row never warns —
        this field is about an already-imported card, not a candidate
        judgement (that's the Wrong Matches worklist's job)."""
        self.db.seed_request(make_request_row(id=903, status="wanted"))
        log_id = self.db.log_download(
            903, outcome="rejected",
            validation_result=_validation_result_blob(
                distance=0.2, scenario="high_distance", valid=False,
                mappings=[
                    TrackMapping(
                        item=HarnessItem(
                            path="00 - Hidden Track.flac", length=237.6),
                        track=HarnessTrackInfo(
                            title="Lost Weekend", length=15.0),
                    ),
                ],
            ),
        )

        status, data = self._get("/api/pipeline/log")

        self.assertEqual(status, 200)
        item = next(row for row in data["log"] if row["id"] == log_id)
        self.assertIsNone(item["track_length_warning"])

    def test_track_length_warning_absent_with_no_candidates(self):
        """A historical row with no candidates/mapping at all must not
        warn — and must not crash the route."""
        self.db.seed_request(make_request_row(id=904, status="imported"))
        log_id = self.db.log_download(
            904, outcome="success", validation_result=None,
        )

        status, data = self._get("/api/pipeline/log")

        self.assertEqual(status, 200)
        item = next(row for row in data["log"] if row["id"] == log_id)
        self.assertIsNone(item["track_length_warning"])

    def test_have_analysis_error_copy_respects_operator_stop(self):
        classified = classify_log_entry(LogEntry(
            outcome="have_analysis_error",
            request_status="unsearchable",
        ))

        self.assertIn("Operator search stop remains in place", classified.verdict)
        self.assertNotIn("future download", classified.verdict)

    def test_have_analysis_branch_does_not_change_existing_outcomes(self):
        expected = {
            "rejected": ("Rejected", "badge-rejected", "#a33"),
            "timeout": ("Failed", "badge-failed", "#a33"),
            "failed": ("Failed", "badge-warn", "#a86f20"),
            "force_import": ("Force imported", "badge-force", "#46a"),
        }
        for outcome, display in expected.items():
            with self.subTest(outcome=outcome):
                classified = classify_log_entry(LogEntry(outcome=outcome))
                self.assertEqual(
                    (classified.badge, classified.badge_class,
                     classified.border_color),
                    display,
                )

    def test_pipeline_log_beets_never_backfills_attempt_have(self):
        import web.server as srv

        beets = FakeBeetsDB()
        beets.set_mbid_detail(
            "test-mbid-0100",
            {
                "beets_format": "MP3",
                "beets_bitrate": 194,
                "beets_avg_bitrate": 288,
            },
        )
        with patch.object(srv, "_beets_db", return_value=beets):
            status, data = self._get("/api/pipeline/log")

        self.assertEqual(status, 200)
        self.assertEqual(data["log"][0]["beets_bitrate"], 194)
        self.assertEqual(data["log"][0]["beets_avg_bitrate"], 288)
        self.assertIsNone(data["log"][0]["existing_format"])
        self.assertIsNone(data["log"][0]["existing_min_bitrate"])

    def test_new_import_never_projects_post_import_current_evidence(self):
        from lib.quality import AudioQualityMeasurement
        from tests.helpers import make_album_quality_evidence

        evidence = make_album_quality_evidence(
            mb_release_id="test-mbid-0100",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=117,
                avg_bitrate_kbps=131,
                median_bitrate_kbps=132,
                format="Opus",
            ),
            codec="opus",
            container="opus",
            storage_format="Opus",
        )
        self.db.upsert_album_quality_evidence(evidence)
        stored = self.db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert stored is not None and stored.id is not None
        self.assertTrue(self.db.set_request_current_evidence(100, stored.id))

        status, data = self._get("/api/pipeline/log")

        self.assertEqual(status, 200)
        self.assertEqual(data["log"][0]["badge"], "Imported")
        self.assertIsNone(data["log"][0]["existing_format"])
        self.assertIsNone(data["log"][0]["existing_min_bitrate"])

    def test_later_current_evidence_never_rewrites_rejected_attempt_have(self):
        from datetime import timedelta

        from lib.quality import AudioQualityMeasurement
        from tests.helpers import make_album_quality_evidence

        log_id = self.db.log_download(
            100,
            outcome="rejected",
            beets_scenario="high_distance",
        )
        attempt = next(row for row in self.db.download_logs if row.id == log_id)
        evidence = make_album_quality_evidence(
            mb_release_id="test-mbid-0100",
            measured_at=attempt.created_at + timedelta(seconds=1),
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=117,
                avg_bitrate_kbps=131,
                median_bitrate_kbps=132,
                format="Opus",
            ),
            codec="opus",
            container="opus",
            storage_format="Opus",
        )
        self.db.upsert_album_quality_evidence(evidence)
        stored = self.db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert stored is not None and stored.id is not None
        self.assertTrue(self.db.set_request_current_evidence(100, stored.id))

        status, data = self._get("/api/pipeline/log")

        self.assertEqual(status, 200)
        item = next(row for row in data["log"] if row["id"] == log_id)
        self.assertIsNone(item["existing_format"])
        self.assertIsNone(item["existing_min_bitrate"])

    def test_pipeline_log_attempt_have_evidence_wins_over_current_beets(self):
        import web.server as srv

        self.db.log_download(
            100,
            outcome="rejected",
            validation_result={
                "scenario": "high_distance",
                "distance": 0.22,
                "wrong_match_triage": {
                    "action": "deleted_reject",
                    "outcome": "deleted",
                    "reason": "suspect_lossless_downgrade",
                    "preview_verdict": "confident_reject",
                    "preview_decision": "downgrade",
                    "stage_chain": ["stage2_import:downgrade"],
                    "current_measurement": {
                        "format": "AAC",
                        "min_bitrate_kbps": 256,
                        "avg_bitrate_kbps": 288,
                    },
                },
            },
        )
        # Canonical request evidence is independently authoritative; the
        # route must not require Beets' lookup to return the album first.
        beets = FakeBeetsDB()
        with patch.object(srv, "_beets_db", return_value=beets):
            status, data = self._get("/api/pipeline/log")

        self.assertEqual(status, 200)
        self.assertEqual(data["log"][0]["existing_format"], "AAC")
        self.assertEqual(data["log"][0]["existing_min_bitrate"], 256)

    def test_measurement_failed_uses_complete_pre_attempt_have_and_diagnostic(self):
        """Badlands: a partial spectral audit cannot hide canonical HAVE."""
        from lib.quality import (
            AudioQualityMeasurement,
            ImportResult,
            SpectralAnalysisDetail,
            SpectralDetail,
        )
        from tests.helpers import make_album_quality_evidence

        evidence = make_album_quality_evidence(
            mb_release_id="test-mbid-0100",
            measured_at=datetime(2026, 7, 1, tzinfo=UTC),
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=90,
                avg_bitrate_kbps=97,
                median_bitrate_kbps=95,
                format="Opus",
                spectral_grade="suspect",
                spectral_subject="installed",
                spectral_provenance="measured",
            ),
            codec="opus",
            container="opus",
            storage_format="Opus",
        )
        self.db.upsert_album_quality_evidence(evidence)
        stored = self.db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert stored is not None and stored.id is not None
        self.assertTrue(self.db.set_request_current_evidence(100, stored.id))

        diagnostic = "ffmpeg decode failed on 05 - Black Nylon.flac"
        log_id = self.db.log_download(
            100,
            outcome="measurement_failed",
            beets_scenario="measurement_failed",
            beets_detail=diagnostic,
            error_message=diagnostic,
            import_result=ImportResult(
                spectral=SpectralDetail(
                    candidate=SpectralAnalysisDetail(
                        attempted=True,
                        grade="error",
                    ),
                    existing=SpectralAnalysisDetail(
                        attempted=True,
                        grade="suspect",
                    ),
                ),
            ).to_json(),
        )

        status, data = self._get("/api/pipeline/log")

        self.assertEqual(status, 200)
        item = next(row for row in data["log"] if row["id"] == log_id)
        self.assertEqual(item["badge"], "Measurement failed")
        self.assertEqual(item["badge_class"], "badge-warn")
        self.assertEqual(item["border_color"], "#a86f20")
        self.assertEqual(item["error_message"], diagnostic)
        self.assertEqual(item["verdict"], f"Measurement failed: {diagnostic}")
        self.assertEqual(item["summary"], f"Measurement failed: {diagnostic}")
        self.assertEqual(item["existing_format"], "Opus")
        self.assertEqual(item["existing_min_bitrate"], 90)
        self.assertEqual(item["existing_avg_bitrate"], 97)
        self.assertEqual(item["existing_median_bitrate"], 95)
        self.assertEqual(item["existing_spectral_grade"], "suspect")
        self.assertFalse(item["existing_spectral_attempted"])
        self.assertIsNone(item["existing_spectral_error"])

        rejected_status, rejected_data = self._get(
            "/api/pipeline/log?outcome=rejected"
        )
        self.assertEqual(rejected_status, 200)
        self.assertIn(log_id, {row["id"] for row in rejected_data["log"]})

    def test_pipeline_log_failed_uses_environment_failure_style(self):
        diagnostic = "beets importer exited 1"
        log_id = self.db.log_download(
            100,
            outcome="failed",
            soulseek_username="testuser",
            error_message=diagnostic,
        )

        status, data = self._get("/api/pipeline/log")

        self.assertEqual(status, 200)
        item = next(row for row in data["log"] if row["id"] == log_id)
        self.assertEqual(item["badge"], "Failed")
        self.assertEqual(item["badge_class"], "badge-warn")
        self.assertEqual(item["border_color"], "#a86f20")
        self.assertEqual(item["verdict"], f"Import error: {diagnostic}")
        self.assertEqual(item["summary"], f"Import error: {diagnostic} · testuser")

    def test_kept_would_import_uses_complete_canonical_current_have(self):
        import web.server as srv
        from lib.quality import (
            AlbumQualityV0Metric,
            AudioQualityMeasurement,
            ImportResult,
            TargetQualityContract,
        )
        from tests.helpers import make_album_quality_evidence

        source_log_id = self.db.log_download(
            100,
            outcome="rejected",
            validation_result={
                "scenario": "high_distance",
                "distance": 0.2328,
                "wrong_match_triage": {
                    "action": "kept_would_import",
                    "outcome": "kept_would_import",
                    "reason": "import",
                    "preview_verdict": "would_import",
                    "preview_decision": "import",
                    "stage_chain": ["stage2_import:import"],
                },
            },
        )
        evidence = make_album_quality_evidence(
            mb_release_id="test-mbid-0100",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=118,
                avg_bitrate_kbps=124,
                median_bitrate_kbps=122,
                format="Opus",
                spectral_grade="likely_transcode",
                spectral_bitrate_kbps=96,
                spectral_subject="source",
                spectral_provenance="carried",
            ),
            v0_metric=AlbumQualityV0Metric(
                min_bitrate_kbps=246,
                avg_bitrate_kbps=258,
                median_bitrate_kbps=257,
                subject="source",
            ),
            codec="opus",
            container="opus",
            storage_format="Opus",
        )
        self.db.upsert_album_quality_evidence(evidence)
        stored = self.db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert stored is not None and stored.id is not None
        self.assertTrue(self.db.set_request_current_evidence(100, stored.id))
        self.db.log_download(
            100,
            outcome="force_import",
            source_download_log_id=source_log_id,
            was_converted=True,
            import_result=ImportResult(
                decision="import",
                source_measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=529,
                    avg_bitrate_kbps=648,
                    median_bitrate_kbps=642,
                    format="FLAC",
                ),
                current_measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=118,
                    avg_bitrate_kbps=124,
                    median_bitrate_kbps=122,
                    format="Opus",
                ),
                target_quality_contract=(
                    TargetQualityContract.from_explicit_label("opus 128")
                ),
                materialized_measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=118,
                    avg_bitrate_kbps=124,
                    median_bitrate_kbps=122,
                    format="Opus",
                ),
            ).to_json(),
        )
        beets = FakeBeetsDB()
        beets.set_mbid_detail(
            "test-mbid-0100",
            {
                "beets_format": "Opus",
                "beets_bitrate": 118,
                "beets_avg_bitrate": 124,
            },
        )
        with patch.object(srv, "_beets_db", return_value=beets):
            status, data = self._get("/api/pipeline/log")

        self.assertEqual(status, 200)
        item = next(
            row for row in data["log"]
            if row["wrong_match_triage_action"] == "kept_would_import"
        )
        self.assertEqual(item["wrong_match_triage_action"], "kept_would_import")
        self.assertEqual(item["badge"], "Triaged · download kept")
        self.assertEqual(item["badge_class"], "badge-warn")
        self.assertEqual(item["border_color"], "#a33")
        self.assertEqual(item["existing_format"], "Opus")
        self.assertEqual(item["existing_min_bitrate"], 118)
        self.assertEqual(item["existing_avg_bitrate"], 124)
        self.assertEqual(item["existing_median_bitrate"], 122)
        self.assertEqual(item["existing_spectral_grade"], "likely_transcode")
        self.assertEqual(item["existing_spectral_bitrate"], 96)
        self.assertEqual(item["existing_v0_probe_kind"], "source")
        self.assertEqual(item["existing_v0_probe_min_bitrate"], 246)
        self.assertEqual(item["existing_v0_probe_avg_bitrate"], 258)
        self.assertEqual(item["materialized_format"], "Opus")
        self.assertEqual(item["materialized_min_bitrate"], 118)
        self.assertEqual(item["materialized_avg_bitrate"], 124)
        self.assertEqual(item["target_contract_format"], "opus 128")

    def test_deleted_triage_uses_complete_canonical_current_have(self):
        import web.server as srv
        from lib.quality import AlbumQualityV0Metric, AudioQualityMeasurement
        from tests.helpers import make_album_quality_evidence

        self.db.log_download(
            100,
            outcome="rejected",
            validation_result={
                "scenario": "high_distance",
                "distance": 0.221,
                "wrong_match_triage": {
                    "action": "deleted_reject",
                    "outcome": "deleted",
                    "reason": "suspect_lossless_downgrade",
                    "preview_verdict": "confident_reject",
                    "preview_decision": "suspect_lossless_downgrade",
                    "stage_chain": ["stage2_import:suspect_lossless_downgrade"],
                },
            },
        )
        evidence = make_album_quality_evidence(
            mb_release_id="test-mbid-0100",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=93,
                avg_bitrate_kbps=129,
                median_bitrate_kbps=128,
                format="Opus",
                spectral_grade="suspect",
                spectral_bitrate_kbps=96,
                spectral_subject="source",
                spectral_provenance="carried",
            ),
            v0_metric=AlbumQualityV0Metric(
                min_bitrate_kbps=193,
                avg_bitrate_kbps=256,
                median_bitrate_kbps=258,
                subject="source",
            ),
            codec="opus",
            container="opus",
            storage_format="Opus",
        )
        self.db.upsert_album_quality_evidence(evidence)
        stored = self.db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert stored is not None and stored.id is not None
        self.assertTrue(self.db.set_request_current_evidence(100, stored.id))

        beets = FakeBeetsDB()
        beets.set_mbid_detail(
            "test-mbid-0100",
            {
                "beets_format": "Opus",
                "beets_bitrate": 93,
                "beets_avg_bitrate": 129,
            },
        )
        with patch.object(srv, "_beets_db", return_value=beets):
            status, data = self._get("/api/pipeline/log")

        self.assertEqual(status, 200)
        item = data["log"][0]
        self.assertEqual(item["badge"], "Triaged · download deleted")
        self.assertEqual(item["existing_format"], "Opus")
        self.assertEqual(item["existing_min_bitrate"], 93)
        self.assertEqual(item["existing_avg_bitrate"], 129)
        self.assertEqual(item["existing_median_bitrate"], 128)
        self.assertEqual(item["existing_spectral_grade"], "suspect")
        self.assertEqual(item["existing_spectral_bitrate"], 96)
        self.assertEqual(item["existing_v0_probe_kind"], "source")
        self.assertEqual(item["existing_v0_probe_min_bitrate"], 193)
        self.assertEqual(item["existing_v0_probe_avg_bitrate"], 256)

    def test_deleted_triage_partial_v0_does_not_suppress_current_have(self):
        """Music for Qigong Dancing: a lone audit V0 is not a HAVE row."""
        import web.server as srv
        from lib.quality import AlbumQualityV0Metric, AudioQualityMeasurement
        from tests.helpers import make_album_quality_evidence

        self.db.log_download(
            100,
            outcome="rejected",
            validation_result={
                "scenario": "high_distance",
                "distance": 0.179,
                "wrong_match_triage": {
                    "action": "deleted_reject",
                    "outcome": "deleted",
                    "reason": "downgrade",
                    "preview_verdict": "confident_reject",
                    "preview_decision": "downgrade",
                    "stage_chain": ["stage2_import:downgrade"],
                    "current_measurement": {
                        "format": None,
                        "min_bitrate_kbps": None,
                        "avg_bitrate_kbps": None,
                        "median_bitrate_kbps": None,
                        "spectral_grade": None,
                        "spectral_bitrate_kbps": None,
                    },
                    "current_v0_probe": {
                        "kind": "on_disk_research_v0",
                        "min_bitrate_kbps": 245,
                        "avg_bitrate_kbps": 268,
                        "median_bitrate_kbps": 268,
                    },
                },
            },
        )
        evidence = make_album_quality_evidence(
            mb_release_id="test-mbid-0100",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=320,
                avg_bitrate_kbps=320,
                median_bitrate_kbps=320,
                format="MP3",
                spectral_grade="genuine",
                spectral_bitrate_kbps=96,
            ),
            v0_metric=AlbumQualityV0Metric(
                min_bitrate_kbps=245,
                avg_bitrate_kbps=268,
                median_bitrate_kbps=268,
                subject="installed",
            ),
            lineage_version=1,
        )
        self.db.upsert_album_quality_evidence(evidence)
        stored = self.db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert stored is not None and stored.id is not None
        self.assertTrue(self.db.set_request_current_evidence(100, stored.id))

        beets = FakeBeetsDB()
        beets.set_mbid_detail(
            "test-mbid-0100",
            {
                "beets_format": "MP3",
                "beets_bitrate": 320,
                "beets_avg_bitrate": 320,
            },
        )
        with patch.object(srv, "_beets_db", return_value=beets):
            status, data = self._get("/api/pipeline/log")

        self.assertEqual(status, 200)
        item = data["log"][0]
        self.assertEqual(item["badge"], "Triaged · download deleted")
        self.assertEqual(item["existing_format"], "MP3")
        self.assertEqual(item["existing_min_bitrate"], 320)
        self.assertEqual(item["existing_avg_bitrate"], 320)
        self.assertEqual(item["existing_median_bitrate"], 320)
        self.assertEqual(item["existing_spectral_grade"], "genuine")
        self.assertEqual(item["existing_spectral_bitrate"], 96)
        self.assertEqual(
            item["existing_v0_probe_kind"], "installed"
        )
        self.assertEqual(item["existing_v0_probe_min_bitrate"], 245)
        self.assertEqual(item["existing_v0_probe_avg_bitrate"], 268)

    def test_kept_would_import_completes_have_from_explicit_successor(self):
        import web.server as srv
        from lib.quality import AudioQualityMeasurement, ImportResult

        source_log_id = self.db.log_download(
            100,
            outcome="rejected",
            validation_result={
                "scenario": "high_distance",
                "distance": 0.172,
                "wrong_match_triage": {
                    "action": "kept_would_import",
                    "outcome": "kept_would_import",
                    "reason": "import",
                    "preview_verdict": "would_import",
                    "preview_decision": "import",
                    "stage_chain": ["stage2_import:import"],
                    "current_measurement": {
                        "format": None,
                        "min_bitrate_kbps": None,
                        "avg_bitrate_kbps": None,
                        "median_bitrate_kbps": None,
                        "spectral_grade": "likely_transcode",
                        "spectral_bitrate_kbps": 160,
                    },
                    "current_v0_probe": {
                        "kind": "on_disk_research_v0",
                        "min_bitrate_kbps": 160,
                        "avg_bitrate_kbps": 241,
                        "median_bitrate_kbps": 251,
                    },
                },
            },
        )
        self.db.log_download(
            100,
            outcome="force_import",
            source_download_log_id=source_log_id,
            was_converted=True,
            import_result=ImportResult(
                decision="import",
                source_measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=732,
                    avg_bitrate_kbps=944,
                    median_bitrate_kbps=961,
                    format="FLAC",
                ),
                current_measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=153,
                    avg_bitrate_kbps=228,
                    median_bitrate_kbps=236,
                    format="MP3",
                    spectral_grade="likely_transcode",
                    spectral_bitrate_kbps=160,
                    spectral_subject="installed",
                    spectral_provenance="measured",
                ),
                materialized_measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=99,
                    avg_bitrate_kbps=128,
                    median_bitrate_kbps=127,
                    format="Opus",
                ),
            ).to_json(),
        )
        beets = FakeBeetsDB()
        beets.set_mbid_detail(
            "test-mbid-0100",
            {
                "beets_format": "Opus",
                "beets_bitrate": 99,
                "beets_avg_bitrate": 128,
            },
        )
        with patch.object(srv, "_beets_db", return_value=beets):
            status, data = self._get("/api/pipeline/log")

        self.assertEqual(status, 200)
        item = next(
            row for row in data["log"]
            if row["wrong_match_triage_action"] == "kept_would_import"
        )
        self.assertEqual(item["existing_format"], "MP3")
        self.assertEqual(item["existing_min_bitrate"], 153)
        self.assertEqual(item["existing_avg_bitrate"], 228)
        self.assertEqual(item["existing_median_bitrate"], 236)
        self.assertEqual(item["existing_spectral_grade"], "likely_transcode")
        self.assertEqual(item["existing_v0_probe_avg_bitrate"], 241)
        self.assertEqual(item["materialized_format"], "Opus")
        self.assertEqual(item["materialized_min_bitrate"], 99)
        self.assertEqual(item["materialized_avg_bitrate"], 128)

        with patch.object(srv, "_beets_db", return_value=beets):
            filtered_status, filtered_data = self._get(
                "/api/pipeline/log?outcome=rejected"
            )

        self.assertEqual(filtered_status, 200)
        filtered_item = next(
            row for row in filtered_data["log"]
            if row["wrong_match_triage_action"] == "kept_would_import"
        )
        self.assertEqual(filtered_item["existing_format"], "MP3")
        self.assertEqual(filtered_item["existing_min_bitrate"], 153)
        self.assertEqual(filtered_item["existing_avg_bitrate"], 228)
        self.assertEqual(filtered_item["existing_median_bitrate"], 236)
        self.assertEqual(
            filtered_item["existing_spectral_grade"], "likely_transcode"
        )
        self.assertEqual(filtered_item["existing_v0_probe_avg_bitrate"], 241)
        self.assertEqual(filtered_item["materialized_format"], "Opus")
        self.assertEqual(filtered_item["materialized_min_bitrate"], 99)
        self.assertEqual(filtered_item["materialized_avg_bitrate"], 128)

    def test_pipeline_log_projects_complete_canonical_candidate_evidence(self):
        from lib.quality import AlbumQualityV0Metric, AudioQualityMeasurement
        from tests.helpers import make_album_quality_evidence

        log_id = self.db.log_download(
            100,
            outcome="rejected",
            validation_result={
                "scenario": "high_distance",
                "distance": 0.2328,
                "wrong_match_triage": {
                    "action": "deleted_reject",
                    "reason": "requeue_upgrade",
                    "preview_verdict": "confident_reject",
                    "preview_decision": "requeue_upgrade",
                    "stage_chain": ["quality:reject"],
                },
            },
        )
        evidence = make_album_quality_evidence(
            mb_release_id="test-mbid-0100",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=201,
                avg_bitrate_kbps=259,
                median_bitrate_kbps=255,
                format="MP3",
                spectral_grade="likely_transcode",
                spectral_bitrate_kbps=96,
            ),
            v0_metric=AlbumQualityV0Metric(
                min_bitrate_kbps=201,
                avg_bitrate_kbps=259,
                median_bitrate_kbps=255,
                subject="installed",
            ),
        )
        self.db.upsert_album_quality_evidence(evidence)
        stored = self.db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert stored is not None and stored.id is not None
        self.db.set_download_log_candidate_evidence(log_id, stored.id)

        status, data = self._get("/api/pipeline/log")

        self.assertEqual(status, 200)
        item = next(row for row in data["log"] if row["id"] == log_id)
        self.assertEqual(item["badge"], "Triaged · download deleted")
        self.assertEqual(item["source_format"], "MP3")
        self.assertEqual(item["source_min_bitrate"], 201)
        self.assertEqual(item["source_avg_bitrate"], 259)
        self.assertEqual(item["source_median_bitrate"], 255)
        self.assertEqual(item["downloaded_label"], "MP3 V2")
        self.assertEqual(item["spectral_grade"], "likely_transcode")
        self.assertEqual(item["spectral_bitrate"], 96)
        self.assertEqual(item["v0_probe_kind"],
                         "native_lossy_research_v0")
        self.assertEqual(item["v0_probe_min_bitrate"], 201)
        self.assertEqual(item["v0_probe_avg_bitrate"], 259)
        self.assertEqual(item["v0_probe_median_bitrate"], 255)

    def test_pipeline_log_withholds_poisoned_candidate_evidence(self):
        from lib.quality import (
            AccurateRipBitMatch,
            AlbumQualityV0Metric,
            AudioQualityMeasurement,
            CdRipBitVerification,
            CdTocIdentity,
        )
        from tests.helpers import make_album_quality_evidence

        cd_rip = CdRipBitVerification(
            toc=CdTocIdentity(
                track_offsets_sectors=[0],
                leadout_sector=470,
                accuraterip_id="000001d6-000003ac-02000601",
                musicbrainz_disc_id="sibling-disc-id",
            ),
            accuraterip=AccurateRipBitMatch(
                provider="accuraterip",
                url="https://www.accuraterip.com/sibling.bin",
                checksum_version="arv1",
                read_offset_samples=0,
                track_confidences=[99],
                track_checksums=[0xDEADBEEF],
                response_sha256="c" * 64,
            ),
        )
        log_id = self.db.log_download(100, outcome="rejected")
        sibling = make_album_quality_evidence(
            mb_release_id="pipeline-log-sibling-pressing",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=201,
                avg_bitrate_kbps=259,
                median_bitrate_kbps=255,
                format="FLAC",
                spectral_grade="likely_transcode",
                spectral_bitrate_kbps=96,
                spectral_subject="source",
                spectral_provenance="measured",
                codec_family="lossless",
                cliff_hz=15000,
                ultrasonic_deficit_db=40.0,
                spectral_measurement_version=2,
            ),
            v0_metric=AlbumQualityV0Metric(
                min_bitrate_kbps=201,
                avg_bitrate_kbps=259,
                median_bitrate_kbps=255,
                subject="source",
            ),
            codec="flac",
            container="flac",
            storage_format="FLAC",
            verified_lossless_proof=cd_rip.verified_lossless_proof(),
            cd_rip_verification=cd_rip,
        )
        self.db.upsert_album_quality_evidence(sibling)
        stored = self.db.find_album_quality_evidence(
            mb_release_id=sibling.mb_release_id,
            snapshot_fingerprint=sibling.snapshot_fingerprint,
        )
        assert stored is not None and stored.id is not None
        self.db.set_download_log_candidate_evidence(log_id, stored.id)

        status, data = self._get("/api/pipeline/log")

        self.assertEqual(status, 200)
        item = next(row for row in data["log"] if row["id"] == log_id)
        for field in (
            "source_format",
            "source_min_bitrate",
            "source_avg_bitrate",
            "source_median_bitrate",
            "spectral_grade",
            "spectral_bitrate",
            "v0_probe_kind",
            "v0_probe_min_bitrate",
            "v0_probe_avg_bitrate",
            "v0_probe_median_bitrate",
            "verified_lossless_classifier",
            "verified_lossless_generation",
            "cd_rip_verification",
            "verdict_tier",
            "verdict_tier_statement",
        ):
            self.assertIsNone(item[field], field)

    def test_pipeline_log_withholds_poisoned_current_evidence(self):
        from lib.quality import AlbumQualityV0Metric, AudioQualityMeasurement
        from tests.helpers import make_album_quality_evidence

        log_id = self.db.log_download(100, outcome="rejected")
        sibling = make_album_quality_evidence(
            mb_release_id="pipeline-log-current-sibling-pressing",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=64,
                avg_bitrate_kbps=72,
                median_bitrate_kbps=70,
                format="AAC",
                spectral_grade="likely_transcode",
                spectral_bitrate_kbps=32,
                spectral_subject="installed",
                spectral_provenance="measured",
                codec_family="aac",
                cliff_hz=12000,
                spectral_measurement_version=2,
            ),
            v0_metric=AlbumQualityV0Metric(
                min_bitrate_kbps=64,
                avg_bitrate_kbps=72,
                median_bitrate_kbps=70,
                subject="installed",
            ),
            codec="aac",
            container="m4a",
            storage_format="AAC",
        )
        self.db.upsert_album_quality_evidence(sibling)
        stored = self.db.find_album_quality_evidence(
            mb_release_id=sibling.mb_release_id,
            snapshot_fingerprint=sibling.snapshot_fingerprint,
        )
        assert stored is not None and stored.id is not None
        self.assertTrue(self.db.set_request_current_evidence(100, stored.id))

        status, data = self._get("/api/pipeline/log")

        self.assertEqual(status, 200)
        item = next(row for row in data["log"] if row["id"] == log_id)
        for field in (
            "existing_format",
            "existing_min_bitrate",
            "existing_avg_bitrate",
            "existing_median_bitrate",
            "existing_spectral_grade",
            "existing_spectral_bitrate",
            "existing_v0_probe_kind",
            "existing_v0_probe_min_bitrate",
            "existing_v0_probe_avg_bitrate",
            "existing_v0_probe_median_bitrate",
            "existing_spectral_accusation_admissible",
        ):
            self.assertIsNone(item[field], field)

    def _seed_verdict_evidence(self, *, log_id, **evidence_kwargs):
        """Attach production-shaped candidate evidence to a download-log row."""
        from tests.helpers import make_album_quality_evidence

        evidence = make_album_quality_evidence(
            mb_release_id="test-mbid-0100", **evidence_kwargs)
        self.db.upsert_album_quality_evidence(evidence)
        stored = self.db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert stored is not None and stored.id is not None
        self.db.set_download_log_candidate_evidence(log_id, stored.id)
        status, data = self._get("/api/pipeline/log")
        self.assertEqual(status, 200)
        return next(row for row in data["log"] if row["id"] == log_id)

    def test_pipeline_log_carries_a_tier_one_proof_gate_verdict(self):
        """A laundered FLAC gets ONE transcode statement (issue #829 PR4)."""
        from lib.quality import AudioQualityMeasurement

        log_id = self.db.log_download(
            100, outcome="rejected", beets_scenario="quality_reject")
        item = self._seed_verdict_evidence(
            log_id=log_id,
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=900,
                avg_bitrate_kbps=950,
                format="FLAC",
                spectral_grade="likely_transcode",
                spectral_bitrate_kbps=128,
                spectral_subject="source",
                spectral_provenance="measured",
                cliff_hz=15500,
                codec_family="lossless",
                spectral_measurement_version=2,
            ),
            codec="flac", container="flac", storage_format="FLAC",
        )
        self.assertEqual(item["verdict_tier"], 1)
        self.assertEqual(
            item["verdict_tier_statement"],
            "Transcode detected: in-window spectral cliff")
        self.assertEqual(item["verdict_fired_legs"], ["in_window_cliff"])
        self.assertTrue(item["spectral_accusation_admissible"])

    def test_pipeline_log_never_accuses_an_audit_only_codec(self):
        """download 37946's shape: a 256 kbps AAC graded likely_transcode."""
        from lib.quality import AudioQualityMeasurement

        log_id = self.db.log_download(
            100, outcome="rejected", beets_scenario="quality_reject")
        item = self._seed_verdict_evidence(
            log_id=log_id,
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=256,
                avg_bitrate_kbps=256,
                is_cbr=True,
                format="AAC",
                spectral_grade="likely_transcode",
                spectral_bitrate_kbps=128,
                spectral_subject="installed",
                spectral_provenance="measured",
                cliff_hz=15000,
                codec_family="aac",
                spectral_measurement_version=2,
            ),
            codec="aac", container="m4a", storage_format="AAC",
        )
        self.assertFalse(item["spectral_accusation_admissible"])
        self.assertEqual(item["verdict_fired_legs"], [])
        # No leg could adjudicate, so the card states nothing rather than
        # reporting a clearance nothing tested for.
        self.assertIsNone(item["verdict_tier"])
        self.assertIsNone(item["verdict_tier_statement"])
        # The measured grade itself is untouched — it stays the audit fact.
        self.assertEqual(item["spectral_grade"], "likely_transcode")

    def test_pipeline_log_never_accuses_an_audit_only_have(self):
        """Request 6387's shape: the INSTALLED copy is the AAC (#829)."""
        from lib.quality import AudioQualityMeasurement
        from tests.helpers import make_album_quality_evidence

        log_id = self.db.log_download(
            100, outcome="rejected", beets_scenario="quality_reject")
        installed = make_album_quality_evidence(
            mb_release_id="test-mbid-0100",
            source_path="/mnt/virtio/Music/Beets/installed",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=256,
                avg_bitrate_kbps=256,
                is_cbr=True,
                format="AAC",
                spectral_grade="likely_transcode",
                spectral_bitrate_kbps=128,
                spectral_subject="installed",
                spectral_provenance="measured",
                cliff_hz=15000,
                codec_family="aac",
                spectral_measurement_version=2,
            ),
            codec="aac", container="m4a", storage_format="AAC",
        )
        self.db.upsert_album_quality_evidence(installed)
        stored = self.db.find_album_quality_evidence(
            mb_release_id=installed.mb_release_id,
            snapshot_fingerprint=installed.snapshot_fingerprint,
        )
        assert stored is not None and stored.id is not None
        self.assertTrue(self.db.set_request_current_evidence(100, stored.id))

        status, data = self._get("/api/pipeline/log")
        self.assertEqual(status, 200)
        item = next(row for row in data["log"] if row["id"] == log_id)
        self.assertEqual(item["existing_spectral_grade"], "likely_transcode")
        self.assertFalse(item["existing_spectral_accusation_admissible"])

    def test_pipeline_log_names_the_proof_generation(self):
        """"verified lossless" stops meaning two things (PR3 constraint 3)."""
        from lib.quality import (
            VERIFIED_LOSSLESS_CLASSIFIER_V3,
            AudioQualityMeasurement,
            VerifiedLosslessProof,
        )

        log_id = self.db.log_download(100, outcome="success")
        item = self._seed_verdict_evidence(
            log_id=log_id,
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=900,
                avg_bitrate_kbps=950,
                format="FLAC",
                spectral_grade="genuine",
                spectral_subject="source",
                spectral_provenance="measured",
                codec_family="lossless",
                ultrasonic_deficit_db=42.0,
                spectral_measurement_version=2,
            ),
            verified_lossless_proof=VerifiedLosslessProof(
                provenance="measured",
                source="flac",
                classifier=VERIFIED_LOSSLESS_CLASSIFIER_V3,
                detail="genuine",
            ),
            codec="flac", container="flac", storage_format="FLAC",
        )
        self.assertEqual(
            item["verified_lossless_classifier"],
            VERIFIED_LOSSLESS_CLASSIFIER_V3)
        self.assertEqual(
            item["verified_lossless_generation"],
            "cliff/grade + ultrasonic legs")
        self.assertEqual(item["verdict_tier"], 5)
        self.assertEqual(
            item["verdict_tier_statement"],
            "No evidence of lossy origin from the tests that ran")
        # A genuine grade has no accusation to withhold, so the flag is
        # not-applicable rather than a codec verdict on the row.
        self.assertIsNone(item["spectral_accusation_admissible"])

    def test_pipeline_log_projects_attributable_cd_rip_provider_audit(self):
        from lib.quality import (
            AccurateRipBitMatch,
            AudioQualityMeasurement,
            CdRipBitVerification,
            CdTocIdentity,
        )

        cd_rip = CdRipBitVerification(
            toc=CdTocIdentity(
                track_offsets_sectors=[0],
                leadout_sector=470,
                accuraterip_id="000001d6-000003ac-02000601",
                musicbrainz_disc_id="exact-disc-id",
            ),
            accuraterip=AccurateRipBitMatch(
                provider="accuraterip",
                url="https://www.accuraterip.com/example.bin",
                checksum_version="arv1",
                read_offset_samples=-78,
                track_confidences=[9],
                track_checksums=[0x12345678],
                response_sha256="a" * 64,
            ),
        )
        log_id = self.db.log_download(100, outcome="success")
        item = self._seed_verdict_evidence(
            log_id=log_id,
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=800,
                format="FLAC",
            ),
            codec="flac",
            container="flac",
            storage_format="FLAC",
            verified_lossless_proof=cd_rip.verified_lossless_proof(),
            cd_rip_verification=cd_rip,
        )

        self.assertEqual(
            item["cd_rip_verification"]["accuraterip"]["read_offset_samples"],
            -78,
        )
        self.assertEqual(
            item["cd_rip_verification"]["accuraterip"]["track_checksums"],
            [0x12345678],
        )
        self.assertEqual(
            item["cd_rip_verification"]["accuraterip"]["response_sha256"],
            "a" * 64,
        )
        self.assertEqual(
            item["verified_lossless_generation"], "exact CD rip bit match"
        )

    def test_disk_coverage_contract(self):
        import web.server as srv

        self.db.seed_request(make_request_row(
            id=9001, status="wanted",
            mb_release_id="00000000-0000-4000-8000-000000009001",
            artist_name="Missing Artist", album_title="Missing Album",
        ))
        beets = FakeBeetsDB()

        with patch.object(srv, "_beets_db", return_value=beets):
            status, data = self._get("/api/disk-coverage")

        self.assertEqual(status, 200)
        _assert_required_fields(
            self, data, {"counts", "off_disk", "inverse"},
            "disk coverage response")
        _assert_required_fields(
            self, data["counts"], self.DISK_COVERAGE_COUNT_FIELDS,
            "disk coverage counts")
        _assert_required_fields(
            self,
            next(row for row in data["off_disk"] if row["id"] == 9001),
            self.DISK_COVERAGE_ROW_FIELDS,
            "disk coverage off-disk row")
        row = next(row for row in data["off_disk"] if row["id"] == 9001)
        self.assertEqual(row["resolution"], {
            "kind": "missing",
        })

    def test_disk_coverage_inverse_contract(self):
        import web.server as srv

        beets = FakeBeetsDB()
        beets.set_release_identities([
            {
                "id": 77,
                "album": "Untracked Album",
                "albumartist": "Untracked Artist",
                "mb_albumid": "beets-only-mbid",
                "discogs_albumid": None,
            },
        ])

        with patch.object(srv, "_beets_db", return_value=beets):
            status, data = self._get("/api/disk-coverage?inverse=1")

        self.assertEqual(status, 200)
        _assert_required_fields(
            self, data["inverse"][0], self.DISK_COVERAGE_INVERSE_FIELDS,
            "disk coverage inverse row")

    def test_pipeline_log_surfaces_wrong_match_triage_audit(self):
        self.db.log_download(
            100, outcome="rejected", soulseek_username="moundsofass",
            validation_result={
                "scenario": "high_distance",
                "distance": 0.190,
                "wrong_match_triage": {
                    "action": "deleted_reject",
                    "reason": "requeue_upgrade",
                    "preview_verdict": "confident_reject",
                    "preview_decision": "requeue_upgrade",
                    "stage_chain": ["mp3_spectral:reject"],
                },
            },
        )

        status, data = self._get("/api/pipeline/log")

        self.assertEqual(status, 200)
        item = data["log"][0]
        self.assertEqual(item["verdict"], "Wrong match (dist 0.190)")
        self.assertEqual(item["summary"],
                         "Wrong match (dist 0.190) · download deleted: "
                         "requeue upgrade · moundsofass")
        self.assertEqual(item["badge"], "Triaged · download deleted")
        self.assertEqual(item["wrong_match_triage_action"], "deleted_reject")
        self.assertIn("requeue upgrade", item["wrong_match_triage_summary"])
        self.assertNotIn("spectral", item["wrong_match_triage_summary"])
        self.assertEqual(item["wrong_match_triage_stage_chain"],
                         ["mp3_spectral:reject"])

    def test_pipeline_status_contract(self):
        status, data = self._get("/api/pipeline/status")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, {"counts", "wanted"}, "pipeline status response")
        _assert_required_fields(self, data["wanted"][0], self.STATUS_WANTED_REQUIRED_FIELDS,
                                "pipeline status wanted item")

    def test_pipeline_all_contract(self):
        self.db.seed_request(make_request_row(
            id=201, status="wanted", album_title="Wanted Album"))

        status, data = self._get("/api/pipeline/all")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, {
            "counts", "wanted", "downloading", "processing", "imported",
            "unsearchable", "imported_total", "imported_truncated",
        },
                                "pipeline all response")
        _assert_required_fields(self, data["wanted"][0], self.PIPELINE_ITEM_REQUIRED_FIELDS,
                                "pipeline all item")

    def test_pipeline_all_imported_is_a_recency_window(self):
        """#426: the imported bucket is capped (newest first) and the
        payload flags the truncation so the UI can say so."""
        from datetime import timedelta

        from web.routes.pipeline import IMPORTED_RECENT_LIMIT
        # setUp already seeded one imported row (id=100); add enough to
        # exceed the cap by 10. Stagger updated_at so newest-first
        # ordering is observable.
        base = datetime(2026, 5, 1, tzinfo=UTC)
        for i in range(IMPORTED_RECENT_LIMIT + 10):
            self.db.seed_request(make_request_row(
                id=1000 + i, status="imported",
                album_title=f"Imported {i}",
                updated_at=base + timedelta(minutes=i),
            ))

        status, data = self._get("/api/pipeline/all")

        self.assertEqual(status, 200)
        self.assertEqual(data["imported_total"], IMPORTED_RECENT_LIMIT + 11)
        self.assertTrue(data["imported_truncated"])
        # The bucket is capped at the limit, newest first.
        self.assertEqual(len(data["imported"]), IMPORTED_RECENT_LIMIT)
        self.assertEqual(data["imported"][0]["album_title"],
                         f"Imported {IMPORTED_RECENT_LIMIT + 9}")

    SEARCH_REQUIRED_FIELDS: ClassVar = {"query", "items", "total"}

    def test_pipeline_search_contract(self):
        self.db.seed_request(make_request_row(
            id=401, status="imported",
            artist_name="The Mountain Goats",
            album_title="Tallahassee"))

        status, data = self._get("/api/pipeline/search?q=mountain")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.SEARCH_REQUIRED_FIELDS,
                                "pipeline search response")
        self.assertEqual(data["query"], "mountain")
        self.assertEqual(data["total"], 1)
        _assert_required_fields(self, data["items"][0],
                                self.PIPELINE_ITEM_REQUIRED_FIELDS,
                                "pipeline search item")

    def test_pipeline_search_blank_query_is_empty(self):
        status, data = self._get("/api/pipeline/search")
        self.assertEqual(status, 200)
        self.assertEqual(data["items"], [])

    def test_processing_owner_projects_on_all_search_and_detail(self):
        request_id = 402
        self.db.seed_request(make_request_row(
            id=request_id,
            status="wanted",
            artist_name="Exact Owner Artist",
            album_title="Exact Owner Album",
            mb_release_id="exact-owner-release",
        ))
        job = handoff_automation_owner(self.db, request_id)
        expected = {
            "job_id": job.id,
            "status": job.status,
            "preview_status": job.preview_status,
        }

        status, all_data = self._get("/api/pipeline/all")
        self.assertEqual(status, 200)
        self.assertEqual(all_data["processing"][0]["processing_owner"], expected)

        status, search_data = self._get(
            "/api/pipeline/search?q=Exact%20Owner"
        )
        self.assertEqual(status, 200)
        self.assertEqual(
            search_data["items"][0]["processing_owner"],
            expected,
        )

        status, detail_data = self._get(f"/api/pipeline/{request_id}")
        self.assertEqual(status, 200)
        self.assertEqual(
            detail_data["request"]["processing_owner"],
            expected,
        )

    def test_acquisition_separates_processing_requests_and_youtube(self):
        processing_id = 403
        self.db.seed_request(make_request_row(
            id=processing_id,
            status="wanted",
            artist_name="Processing Artist",
            album_title="Processing Album",
            mb_release_id="processing-release",
        ))
        job = handoff_automation_owner(self.db, processing_id)
        self.db.insert_youtube_running(
            request_id=processing_id,
            browse_id="processing-youtube",
            audio_playlist_id=None,
            yt_url="https://music.youtube.com/playlist?list=processing",
            expected_track_count=2,
        )
        downloading_id = 404
        self.db.seed_request(make_request_row(
            id=downloading_id,
            status="downloading",
            artist_name="Downloading Artist",
            album_title="Downloading Album",
            mb_release_id="downloading-release",
        ))

        status, data = self._get("/api/pipeline/acquisition")

        self.assertEqual(status, 200)
        self.assertEqual(set(data), {"acquisition", "youtube_ingest"})
        by_id = {row["id"]: row for row in data["acquisition"]}
        self.assertEqual(set(by_id), {processing_id, downloading_id})
        self.assertEqual(by_id[processing_id]["processing_owner"], {
            "job_id": job.id,
            "status": job.status,
            "preview_status": job.preview_status,
        })
        self.assertIsNone(by_id[downloading_id]["processing_owner"])
        self.assertEqual(len(data["youtube_ingest"]), 1)
        self.assertEqual(
            data["youtube_ingest"][0]["request_status"],
            "processing",
        )
        self.assertIsNone(
            data["youtube_ingest"][0]["processing_owner"],
        )
        self.assertEqual(self.db.query_counts["get_acquisition"], 1)

    DETAIL_RESPONSE_REQUIRED_FIELDS: ClassVar = {
        "request", "history", "tracks", "last_search", "current_library",
    }
    #: Issue #829 Phase 5 PR4 — the detail header's Quality row picks its
    #: grade from a chain over BOTH the installed copy and the last
    #: download, so it needs BOTH audit-only pairs and applies whichever
    #: matches the grade it selected. Detail-only: the queue routes that
    #: share ``PIPELINE_ITEM_REQUIRED_FIELDS`` render no spectral chip.
    DETAIL_ACCUSATION_REQUIRED_FIELDS: ClassVar = {
        "current_spectral_accusation_admissible",
        "current_spectral_accusation_withheld",
        "current_cd_rip_verification",
        "last_download_spectral_accusation_admissible",
        "last_download_spectral_accusation_withheld",
    }
    LAST_SEARCH_REQUIRED_FIELDS: ClassVar = {
        "variant", "final_state", "outcome", "top_candidates",
    }
    CANDIDATE_SCORE_REQUIRED_FIELDS: ClassVar = {
        "username", "dir", "filetype", "matched_tracks", "total_tracks",
        "avg_ratio", "missing_titles", "file_count",
    }

    def test_pipeline_detail_contract(self):
        status, data = self._get("/api/pipeline/100")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.DETAIL_RESPONSE_REQUIRED_FIELDS,
                                "pipeline detail response")
        _assert_required_fields(self, data["request"], self.PIPELINE_ITEM_REQUIRED_FIELDS,
                                "pipeline detail request")
        _assert_required_fields(
            self, data["request"], self.DETAIL_ACCUSATION_REQUIRED_FIELDS,
            "pipeline detail request audit-only flags")
        _assert_required_fields(self, data["history"][0], self.HISTORY_REQUIRED_FIELDS,
                                "pipeline detail history item")
        # Default mock state: no search history → last_search is None.
        self.assertIsNone(data["last_search"])

    def _seed_installed_evidence(self, measurement, **evidence_kwargs):
        """Link a production-shaped installed evidence row to request 100."""
        from tests.helpers import make_album_quality_evidence

        installed = make_album_quality_evidence(
            mb_release_id="test-mbid-0100",
            source_path="/mnt/virtio/Music/Beets/installed",
            measurement=measurement,
            **evidence_kwargs,
        )
        self.db.upsert_album_quality_evidence(installed)
        stored = self.db.find_album_quality_evidence(
            mb_release_id=installed.mb_release_id,
            snapshot_fingerprint=installed.snapshot_fingerprint,
        )
        assert stored is not None and stored.id is not None
        self.assertTrue(self.db.set_request_current_evidence(100, stored.id))
        return stored

    def test_pipeline_detail_projects_current_cd_rip_provider_audit(self):
        from lib.quality import (
            AccurateRipBitMatch,
            AudioQualityMeasurement,
            CdRipBitVerification,
            CdTocIdentity,
        )

        cd_rip = CdRipBitVerification(
            toc=CdTocIdentity(
                track_offsets_sectors=[0],
                leadout_sector=470,
                accuraterip_id="000001d6-000003ac-02000601",
                musicbrainz_disc_id="exact-disc-id",
            ),
            accuraterip=AccurateRipBitMatch(
                provider="accuraterip",
                url="https://www.accuraterip.com/example.bin",
                checksum_version="arv2",
                read_offset_samples=108,
                track_confidences=[42],
                track_checksums=[0x12345678],
                response_sha256="a" * 64,
            ),
        )
        self._seed_installed_evidence(
            AudioQualityMeasurement(min_bitrate_kbps=800, format="FLAC"),
            codec="flac",
            container="flac",
            storage_format="FLAC",
            verified_lossless_proof=cd_rip.verified_lossless_proof(),
            cd_rip_verification=cd_rip,
        )

        status, data = self._get("/api/pipeline/100")

        self.assertEqual(status, 200)
        projected = data["request"]["current_cd_rip_verification"]
        self.assertEqual(projected["algorithm"], "cd-rip-bit-verifier-v1")
        self.assertEqual(
            projected["accuraterip"]["track_confidences"], [42]
        )
        self.assertEqual(
            projected["accuraterip"]["read_offset_samples"], 108
        )
        self.assertEqual(
            projected["accuraterip"]["track_checksums"], [0x12345678]
        )
        self.assertEqual(
            projected["accuraterip"]["response_sha256"], "a" * 64
        )

    def test_pipeline_detail_withholds_poisoned_sibling_evidence_link(self):
        from lib.quality import (
            AccurateRipBitMatch,
            AudioQualityMeasurement,
            CdRipBitVerification,
            CdTocIdentity,
        )
        from tests.helpers import make_album_quality_evidence

        cd_rip = CdRipBitVerification(
            toc=CdTocIdentity(
                track_offsets_sectors=[0],
                leadout_sector=470,
                accuraterip_id="000001d6-000003ac-02000601",
                musicbrainz_disc_id="sibling-disc-id",
            ),
            accuraterip=AccurateRipBitMatch(
                provider="accuraterip",
                url="https://www.accuraterip.com/sibling.bin",
                checksum_version="arv1",
                read_offset_samples=0,
                track_confidences=[99],
                track_checksums=[0xDEADBEEF],
                response_sha256="c" * 64,
            ),
        )
        sibling = make_album_quality_evidence(
            mb_release_id="sibling-pressing-mbid",
            source_path="/mnt/virtio/Music/Beets/sibling",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=128,
                format="MP3",
                spectral_grade="likely_transcode",
                spectral_subject="source",
                spectral_provenance="measured",
                codec_family="mp3",
            ),
            codec="mp3",
            container="mp3",
            storage_format="MP3",
            verified_lossless_proof=cd_rip.verified_lossless_proof(),
            cd_rip_verification=cd_rip,
        )
        self.db.upsert_album_quality_evidence(sibling)
        stored = self.db.find_album_quality_evidence(
            mb_release_id=sibling.mb_release_id,
            snapshot_fingerprint=sibling.snapshot_fingerprint,
        )
        assert stored is not None and stored.id is not None
        self.assertTrue(self.db.set_request_current_evidence(100, stored.id))

        status, data = self._get("/api/pipeline/100")

        self.assertEqual(status, 200)
        request = data["request"]
        self.assertIsNone(request["current_cd_rip_verification"])
        self.assertFalse(request["current_spectral_accusation_admissible"])
        self.assertIsNone(request["current_spectral_accusation_withheld"])

    def test_pipeline_detail_does_not_treat_two_empty_release_ids_as_exact(self):
        from lib.quality import AudioQualityMeasurement
        from tests.helpers import make_album_quality_evidence

        poisoned = make_album_quality_evidence(
            mb_release_id="temporary-valid-id",
            source_path="/mnt/virtio/Music/Beets/invalid-identity",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=128,
                format="MP3",
                spectral_grade="likely_transcode",
                spectral_subject="installed",
                spectral_provenance="measured",
                codec_family="mp3",
            ),
            codec="mp3",
            container="mp3",
            storage_format="MP3",
        )
        self.db.upsert_album_quality_evidence(poisoned)
        stored = self.db.find_album_quality_evidence(
            mb_release_id=poisoned.mb_release_id,
            snapshot_fingerprint=poisoned.snapshot_fingerprint,
        )
        assert stored is not None and stored.id is not None
        self.db._evidence_by_id[stored.id] = msgspec.structs.replace(
            stored,
            mb_release_id="",
        )
        self.db._requests[100]["mb_release_id"] = ""
        self.assertTrue(self.db.set_request_current_evidence(100, stored.id))

        status, data = self._get("/api/pipeline/100")

        self.assertEqual(status, 200)
        request = data["request"]
        self.assertFalse(request["current_spectral_accusation_admissible"])
        self.assertIsNone(request["current_spectral_accusation_withheld"])
        self.assertIsNone(request["current_cd_rip_verification"])

    def test_pipeline_detail_withholds_the_have_accusation_for_audit_only(self):
        """Request 6387's shape at the detail header: the INSTALLED copy is
        the AAC the codec-blind analyzer graded ``likely_transcode``."""
        from lib.quality import AudioQualityMeasurement

        self._seed_installed_evidence(
            AudioQualityMeasurement(
                min_bitrate_kbps=256, avg_bitrate_kbps=256, is_cbr=True,
                format="AAC", spectral_grade="likely_transcode",
                spectral_bitrate_kbps=128, spectral_subject="installed",
                spectral_provenance="measured", cliff_hz=15000,
                codec_family="aac", spectral_measurement_version=2,
            ),
            codec="aac", container="m4a", storage_format="AAC",
        )

        status, data = self._get("/api/pipeline/100")

        self.assertEqual(status, 200)
        self.assertIs(
            data["request"]["current_spectral_accusation_admissible"], False)
        self.assertEqual(
            data["request"]["current_spectral_accusation_withheld"],
            "audit_only_codec")

    def test_pipeline_detail_keeps_the_have_accusation_for_a_real_codec(self):
        """The must-still-work half: a LAME MP3 cliff still accuses."""
        from lib.quality import AudioQualityMeasurement

        self._seed_installed_evidence(
            AudioQualityMeasurement(
                min_bitrate_kbps=320, avg_bitrate_kbps=320, is_cbr=True,
                format="MP3", spectral_grade="likely_transcode",
                spectral_bitrate_kbps=128, spectral_subject="installed",
                spectral_provenance="measured", cliff_hz=16000,
                codec_family="mp3", spectral_measurement_version=2,
            ),
            codec="mp3", container="mp3", storage_format="MP3",
        )

        status, data = self._get("/api/pipeline/100")

        self.assertEqual(status, 200)
        self.assertIs(
            data["request"]["current_spectral_accusation_admissible"], True)
        self.assertIsNone(
            data["request"]["current_spectral_accusation_withheld"])

    def test_pipeline_detail_reports_an_unresolved_codec_separately(self):
        """An unresolved family may not be described as encoder rolloff."""
        from lib.quality import AudioQualityMeasurement

        self._seed_installed_evidence(
            AudioQualityMeasurement(
                min_bitrate_kbps=192, avg_bitrate_kbps=192,
                format=None, spectral_grade="suspect",
                spectral_bitrate_kbps=192, spectral_subject="installed",
                spectral_provenance="measured", cliff_hz=18000,
                codec_family=None, spectral_measurement_version=2,
            ),
            codec=None, container=None, storage_format=None,
        )

        status, data = self._get("/api/pipeline/100")

        self.assertEqual(status, 200)
        self.assertIs(
            data["request"]["current_spectral_accusation_admissible"], False)
        self.assertEqual(
            data["request"]["current_spectral_accusation_withheld"],
            "codec_unresolved")

    def test_pipeline_detail_have_flags_are_absent_without_evidence(self):
        """Fail-accusing: no linked current evidence, no flags, so the
        header keeps its historical accusing render."""
        status, data = self._get("/api/pipeline/100")

        self.assertEqual(status, 200)
        self.assertIsNone(
            data["request"]["current_spectral_accusation_admissible"])
        self.assertIsNone(
            data["request"]["current_spectral_accusation_withheld"])
        self.assertIsNone(
            data["request"]["last_download_spectral_accusation_admissible"])

    def test_pipeline_detail_last_download_flags_track_the_denorm_grade(self):
        """The candidate half of the chain: the flags must belong to the
        attempt whose grade ``last_download_spectral_grade`` copied."""
        from lib.quality import AudioQualityMeasurement
        from tests.helpers import make_album_quality_evidence

        candidate = make_album_quality_evidence(
            mb_release_id="test-mbid-0100",
            source_path="/mnt/virtio/music/slskd/candidate",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=256, avg_bitrate_kbps=256, is_cbr=True,
                format="AAC", spectral_grade="likely_transcode",
                spectral_bitrate_kbps=128, spectral_subject="source",
                spectral_provenance="measured", cliff_hz=15000,
                codec_family="aac", spectral_measurement_version=2,
            ),
            codec="aac", container="m4a", storage_format="AAC",
        )
        self.db.upsert_album_quality_evidence(candidate)
        stored = self.db.find_album_quality_evidence(
            mb_release_id=candidate.mb_release_id,
            snapshot_fingerprint=candidate.snapshot_fingerprint,
        )
        assert stored is not None and stored.id is not None
        log_id = self.db.log_download(
            100, outcome="rejected", beets_scenario="quality_reject")
        self.db.set_download_log_candidate_evidence(log_id, stored.id)
        self.db.update_request_fields(
            100, last_download_spectral_grade="likely_transcode")

        status, data = self._get("/api/pipeline/100")

        self.assertEqual(status, 200)
        self.assertIs(
            data["request"][
                "last_download_spectral_accusation_admissible"], False)
        self.assertEqual(
            data["request"]["last_download_spectral_accusation_withheld"],
            "audit_only_codec")

    def test_pipeline_detail_last_download_flags_drop_on_a_grade_mismatch(self):
        """Fail-accusing: when the denorm names a grade no retained attempt
        measured, the pair is empty rather than a different album's."""
        from lib.quality import AudioQualityMeasurement
        from tests.helpers import make_album_quality_evidence

        candidate = make_album_quality_evidence(
            mb_release_id="test-mbid-0100",
            source_path="/mnt/virtio/music/slskd/candidate",
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=256, avg_bitrate_kbps=256, is_cbr=True,
                format="AAC", spectral_grade="likely_transcode",
                spectral_bitrate_kbps=128, spectral_subject="source",
                spectral_provenance="measured", cliff_hz=15000,
                codec_family="aac", spectral_measurement_version=2,
            ),
            codec="aac", container="m4a", storage_format="AAC",
        )
        self.db.upsert_album_quality_evidence(candidate)
        stored = self.db.find_album_quality_evidence(
            mb_release_id=candidate.mb_release_id,
            snapshot_fingerprint=candidate.snapshot_fingerprint,
        )
        assert stored is not None and stored.id is not None
        log_id = self.db.log_download(
            100, outcome="rejected", beets_scenario="quality_reject")
        self.db.set_download_log_candidate_evidence(log_id, stored.id)
        # The denorm names a DIFFERENT grade than any retained attempt.
        self.db.update_request_fields(
            100, last_download_spectral_grade="suspect")

        status, data = self._get("/api/pipeline/100")

        self.assertEqual(status, 200)
        self.assertIsNone(
            data["request"]["last_download_spectral_accusation_admissible"])
        self.assertIsNone(
            data["request"]["last_download_spectral_accusation_withheld"])

    def test_pipeline_detail_history_derives_apply_beets_distance(self):
        """Issue #865: the apply-time beets distance persisted by #863 in
        ``import_result`` JSONB must surface as a first-class history field
        (the card previously showed only the validate-time distance)."""
        self.db.log_download(
            request_id=100,
            outcome="rejected",
            soulseek_username="SevenNines",
            error_message="beets apply distance 0.5637 exceeded 0.5",
            import_result={
                "version": 4,
                "exit_code": 2,
                "decision": "import_failed",
                "apply_beets_distance": 0.5637,
            },
        )
        status, data = self._get("/api/pipeline/100")
        self.assertEqual(status, 200)
        by_msg = [
            h for h in data["history"]
            if h.get("error_message") == "beets apply distance 0.5637 exceeded 0.5"
        ]
        self.assertEqual(len(by_msg), 1)
        self.assertEqual(by_msg[0]["apply_beets_distance"], 0.5637)

    def test_pipeline_detail_history_humanizes_peer_refusals(self):
        """Issue #868: download_log 38272's shape — 29 files, one peer, all
        ``Completed, Rejected`` at zero bytes with the peer exception
        ``Verification required``. The card must say the PEER refused, and
        must still show the peer's own words (``transfer_detail`` itself is
        log-only by contract, so this projection is the only place they
        reach the operator)."""
        self.db.log_download(
            request_id=100,
            outcome="timeout",
            soulseek_username="Tymemage",
            error_message="all 29 files errored — 29× 'Verification required'",
            transfer_detail=[
                {
                    "username": "Tymemage",
                    "filename": f"@@share\\Beefeater\\{index:02d} - Track.flac",
                    "last_state": "Completed, Rejected",
                    "last_exception": "Verification required",
                    "bytes_transferred": 0,
                    "retry_count": 0,
                }
                for index in range(1, 30)
            ],
        )

        status, data = self._get("/api/pipeline/100")

        self.assertEqual(status, 200)
        item = next(
            row for row in data["history"] if row["outcome"] == "timeout")
        _assert_required_fields(self, item, self.HISTORY_REQUIRED_FIELDS,
                                "pipeline detail history item")
        self.assertEqual(
            item["verdict"],
            'Peer Tymemage rejected all 29 files before transfer '
            '— "Verification required"',
        )
        self.assertEqual(item["transfer_message"], '29× "Verification required"')
        self.assertEqual(item["transfer_message_label"], "Peer message")
        # The raw audit column stays log-only; the row still carries the
        # untouched persisted message.
        self.assertNotIn("transfer_detail", item)
        self.assertEqual(
            item["error_message"],
            "all 29 files errored — 29× 'Verification required'",
        )

    def test_pipeline_detail_history_blames_storage_not_the_peer(self):
        """Issue #868: slskd failing to write to OUR share is not peer
        behaviour — neither the verdict nor the evidence label may say so."""
        self.db.log_download(
            request_id=100,
            outcome="timeout",
            soulseek_username="Tymemage",
            error_message="all 3 files errored",
            transfer_detail=[
                {
                    "username": "Tymemage",
                    "filename": f"{index:02d} - Track.flac",
                    "last_state": "Completed, Errored",
                    "last_exception": (
                        f"Failed to create file {index:02d} - Track.flac: "
                        "Stale file handle : "
                        "'/mnt/virtio/music/slskd/incomplete/x'"
                    ),
                    "bytes_transferred": 0,
                    "retry_count": 0,
                }
                for index in range(1, 4)
            ],
        )

        status, data = self._get("/api/pipeline/100")

        self.assertEqual(status, 200)
        item = next(
            row for row in data["history"] if row["outcome"] == "timeout")
        self.assertTrue(
            str(item["verdict"]).startswith("Local storage error writing 3 files"),
            item["verdict"],
        )
        self.assertNotIn("Tymemage", str(item["verdict"]))
        self.assertEqual(item["transfer_message_label"], "Storage error")

    def test_pipeline_detail_history_never_hides_storage_behind_a_giveup(self):
        """Issue #868 I6, from live-data review: the retry-limit headline used
        to be the WHOLE verdict, so 10 of 14 local-storage rows rendered as
        "Gave up on <file> after 5 retries" — an operator reads that
        as a flaky peer and retries the peer, while our own share is what
        failed. The cause is appended, never suppressed (live row 38203)."""
        self.db.log_download(
            request_id=100,
            outcome="timeout",
            soulseek_username="Tymemage",
            error_message=(
                "file exceeded retry limit after 5 retries: "
                "Master\\Jimmy Eat World\\[1996] Static Prevails\\"
                "05 Seventeen.flac"
            ),
            transfer_detail=[{
                "username": "Tymemage",
                "filename": "05 Seventeen.flac",
                "last_state": "Completed, Errored",
                "last_exception": (
                    "Failed to create file 05 Seventeen.flac: Stale file "
                    "handle : '/mnt/virtio/music/slskd/incomplete/x'"
                ),
                "bytes_transferred": 0,
                "retry_count": 5,
            }],
        )

        status, data = self._get("/api/pipeline/100")

        self.assertEqual(status, 200)
        item = next(
            row for row in data["history"] if row["outcome"] == "timeout")
        self.assertEqual(
            item["verdict"],
            'Gave up on "05 Seventeen.flac" after 5 retries '
            "— local storage error writing 1 file",
        )
        self.assertNotIn("Tymemage", str(item["verdict"]))
        self.assertEqual(item["transfer_message_label"], "Storage error")

    def test_pipeline_detail_history_apply_distance_null_for_legacy_rows(self):
        """Rows predating #863 have no apply_beets_distance key — the derived
        field must be null, never an error."""
        status, data = self._get("/api/pipeline/100")
        self.assertEqual(status, 200)
        self.assertIsNone(data["history"][0]["apply_beets_distance"])

    def test_apply_beets_distance_derivation_table(self):
        """Direct coverage of the tolerant JSONB read — the bool guard is
        load-bearing (isinstance(True, int) is True) and the str branch
        covers legacy text-JSONB rows."""
        from web.download_history_view import _apply_beets_distance

        cases = [
            ("dict with float", {"apply_beets_distance": 0.5637}, 0.5637),
            ("dict with int", {"apply_beets_distance": 1}, 1.0),
            ("missing key", {"decision": "import_failed"}, None),
            ("bool must not coerce", {"apply_beets_distance": True}, None),
            ("json string row", '{"apply_beets_distance": 0.25}', 0.25),
            ("malformed json string", "{not json", None),
            ("null jsonb", None, None),
            ("non-dict json", [1, 2], None),
            ("string value", {"apply_beets_distance": "0.5"}, None),
        ]
        for desc, raw, expected in cases:
            with self.subTest(desc=desc):
                self.assertEqual(_apply_beets_distance(raw), expected)

    def test_pipeline_detail_uses_fresh_typed_beets_path(self):
        """The request cache is never a current-library display authority."""
        import web.server as srv

        self.db.request(100)["mb_release_id"] = (
            "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
        )
        beets = FakeBeetsDB(library_root="/current/library")
        beets.set_album_ids_for_release(
            "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", [9001],
        )
        beets.set_item_paths(
            "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            [(91, "/current/library/Moved/01 Track.flac")],
        )
        prior_beets = srv._beets
        srv._beets = beets
        try:
            status, data = self._get("/api/pipeline/100")
        finally:
            srv._beets = prior_beets

        self.assertEqual(status, 200)
        self.assertNotIn("imported_path", data["request"])
        self.assertEqual(data["current_library"], {
            "state": "unique",
            "release_source": "musicbrainz",
            "release_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "album_id": 9001,
            "path": "/current/library/Moved",
        })

    def test_pipeline_detail_exposes_missing_and_ambiguous_authority(self):
        """Missing and ambiguous are operator-visible, never empty paths."""
        import web.server as srv

        self.db.request(100)["mb_release_id"] = (
            "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
        )
        beets = FakeBeetsDB()
        prior_beets = srv._beets
        srv._beets = beets
        try:
            status, missing = self._get("/api/pipeline/100")
        finally:
            srv._beets = prior_beets
        self.assertEqual(status, 200)
        self.assertEqual(missing["current_library"]["state"], "missing")
        self.assertNotIn("path", missing["current_library"])

        beets.set_album_ids_for_release(
            "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", [7, 8],
        )
        prior_beets = srv._beets
        srv._beets = beets
        try:
            status, ambiguous = self._get("/api/pipeline/100")
        finally:
            srv._beets = prior_beets
        self.assertEqual(status, 200)
        self.assertEqual(ambiguous["current_library"], {
            "state": "ambiguous",
            "release_source": "musicbrainz",
            "release_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "reason": "multiple_matches",
            "album_ids": [7, 8],
        })

    def test_pipeline_detail_conflicting_request_identity_is_unavailable(self):
        """Two distinct request identities require manual review."""
        import web.server as srv

        self.db.request(100)["mb_release_id"] = (
            "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
        )
        self.db.request(100)["discogs_release_id"] = "12856590"
        beets = FakeBeetsDB()
        prior_beets = srv._beets
        srv._beets = beets
        try:
            status, data = self._get("/api/pipeline/100")
        finally:
            srv._beets = prior_beets
        self.assertEqual(status, 200)
        self.assertEqual(data["current_library"], {
            "state": "unavailable",
            "reason": "conflicting_request_identity",
            "manual_review": True,
        })
        self.assertEqual(beets.resolve_current_release_calls, [])

    def test_pipeline_detail_surfaces_last_search_top_candidates(self):
        """When the latest search_log row has candidates, the route emits the
        full slice (up to 20) by (matched_tracks DESC, avg_ratio DESC) via
        msgspec.to_builtins."""
        from lib.quality import CandidateScore
        candidates_blob = msgspec.convert([
            {"username": "u1", "dir": "A", "filetype": "flac",
             "matched_tracks": 26, "total_tracks": 26, "avg_ratio": 0.95,
             "missing_titles": [], "file_count": 26},
            {"username": "u2", "dir": "B", "filetype": "mp3",
             "matched_tracks": 22, "total_tracks": 26, "avg_ratio": 0.80,
             "missing_titles": ["x"], "file_count": 22},
            {"username": "u3", "dir": "C", "filetype": "flac",
             "matched_tracks": 26, "total_tracks": 26, "avg_ratio": 0.85,
             "missing_titles": [], "file_count": 26},
            {"username": "u4", "dir": "D", "filetype": "flac",
             "matched_tracks": 20, "total_tracks": 26, "avg_ratio": 0.99,
             "missing_titles": ["a", "b"], "file_count": 20},
        ], type=list[CandidateScore])
        self.db.log_search(
            100, query="*rtist Album", result_count=100, elapsed_s=1.2,
            outcome="no_match", candidates=candidates_blob,
            variant="v3_artist_only", final_state="Completed",
        )

        status, data = self._get("/api/pipeline/100")

        self.assertEqual(status, 200)
        last = data["last_search"]
        self.assertIsNotNone(last)
        _assert_required_fields(self, last, self.LAST_SEARCH_REQUIRED_FIELDS,
                                "last_search payload")
        self.assertEqual(last["variant"], "v3_artist_only")
        self.assertEqual(last["final_state"], "Completed")
        self.assertEqual(last["outcome"], "no_match")
        # All 4 (≤20 cap), sorted by (matched_tracks DESC, avg_ratio DESC):
        # u1 (26, 0.95) → u3 (26, 0.85) → u2 (22, 0.80) → u4 (20, 0.99)
        usernames = [c["username"] for c in last["top_candidates"]]
        self.assertEqual(usernames, ["u1", "u3", "u2", "u4"])
        for cand in last["top_candidates"]:
            _assert_required_fields(self, cand,
                                    self.CANDIDATE_SCORE_REQUIRED_FIELDS,
                                    "candidate score")

    def test_pipeline_detail_caps_top_candidates_at_twenty(self):
        """U2: the peers panel widened from 3 to the full stored cap (20). A
        search row with >20 candidates surfaces exactly 20, still ranked."""
        from lib.quality import CandidateScore
        blob = msgspec.convert([
            {"username": f"u{i:02d}", "dir": f"D{i}", "filetype": "flac",
             "matched_tracks": 26, "total_tracks": 26,
             "avg_ratio": 1.0 - i / 100.0,
             "missing_titles": [], "file_count": 26}
            for i in range(25)
        ], type=list[CandidateScore])
        self.db.log_search(
            100, query="q", result_count=100, elapsed_s=1.0,
            outcome="no_match", candidates=blob,
            variant="v3_artist_only", final_state="Completed",
        )
        status, data = self._get("/api/pipeline/100")
        self.assertEqual(status, 200)
        top = data["last_search"]["top_candidates"]
        self.assertEqual(len(top), 20)
        # All matched_tracks equal → highest avg_ratio first: u00..u19
        self.assertEqual(top[0]["username"], "u00")
        self.assertEqual(top[-1]["username"], "u19")

    def test_pipeline_detail_handles_null_candidates_gracefully(self):
        """Historical search_log row with NULL candidates → top_candidates=[]."""
        self.db.log_search(
            100, query="q", result_count=None, elapsed_s=None,
            outcome="timeout", candidates=None,
            variant=None, final_state=None,
        )
        status, data = self._get("/api/pipeline/100")

        self.assertEqual(status, 200)
        self.assertIsNotNone(data["last_search"])
        self.assertEqual(data["last_search"]["top_candidates"], [])
        self.assertIsNone(data["last_search"]["variant"])

    def test_pipeline_detail_handles_empty_candidates_list(self):
        """Latest search row with an empty candidates list → top_candidates=[]."""
        self.db.log_search(
            100, query="q", result_count=0, elapsed_s=0.5,
            outcome="no_results", candidates=[],
            variant="v2_artist_album_no_year", final_state="Completed",
        )
        status, data = self._get("/api/pipeline/100")

        self.assertEqual(status, 200)
        self.assertEqual(data["last_search"]["top_candidates"], [])
        self.assertEqual(data["last_search"]["variant"], "v2_artist_album_no_year")

    def test_pipeline_detail_handles_malformed_candidates_blob(self):
        """Corrupted search_log.candidates JSONB → 200 with top_candidates=[].

        Guard the route against historical rows whose JSONB shape no longer
        matches CandidateScore. The CLI already wraps msgspec.convert in
        try/except msgspec.ValidationError; the web route must do the same so
        a corrupt row does not 500 the detail page.
        """
        import json as _json
        self.db.log_search(
            100, query="q", result_count=5, elapsed_s=0.5,
            outcome="no_match", candidates=[],
            variant="v2_artist_album_no_year", final_state="Completed",
        )
        # Corrupt the stored JSONB in place — historical rows whose
        # shape predates CandidateScore. The fake stores the encoded
        # JSON string exactly like the real column.
        self.db.search_logs[-1].candidates = _json.dumps([{"foo": "bar"}])
        status, data = self._get("/api/pipeline/100")

        self.assertEqual(status, 200)
        self.assertIsNotNone(data["last_search"])
        self.assertEqual(data["last_search"]["top_candidates"], [])
        self.assertEqual(data["last_search"]["variant"],
                         "v2_artist_album_no_year")

    def test_pipeline_detail_history_surfaces_wrong_match_triage_audit(self):
        self.db.log_download(
            100, outcome="rejected", beets_scenario="high_distance",
            beets_distance=0.190,
            validation_result={
                "wrong_match_triage": {
                    "action": "deleted_reject",
                    "reason": "requeue_upgrade",
                    "preview_verdict": "confident_reject",
                    "preview_decision": "requeue_upgrade",
                    "stage_chain": ["stage1_spectral:reject"],
                },
            },
        )
        status, data = self._get("/api/pipeline/100")

        self.assertEqual(status, 200)
        item = data["history"][0]
        self.assertEqual(item["badge"], "Triaged · download deleted")
        self.assertEqual(item["wrong_match_triage_action"], "deleted_reject")
        self.assertIn("requeue upgrade", item["wrong_match_triage_summary"])
        self.assertNotIn("spectral", item["wrong_match_triage_summary"])
        self.assertEqual(item["wrong_match_triage_preview_verdict"],
                         "confident_reject")
        self.assertEqual(item["wrong_match_triage_stage_chain"],
                         ["stage1_spectral:reject"])

    def test_import_preview_values_contract(self):
        status, data = self._post("/api/import-preview", {
            "values": {
                "is_flac": False,
                "min_bitrate": 320,
                "is_cbr": True,
            },
        })

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.IMPORT_PREVIEW_REQUIRED_FIELDS,
                                "import preview response")
        self.assertEqual(data["mode"], "values")

    def test_import_preview_rejects_ambiguous_modes(self):
        status, data = self._post("/api/import-preview", {
            "values": {"min_bitrate": 320},
            "download_log_id": 1,
        })

        self.assertEqual(status, 400)
        self.assertIn("error", data)

    def test_import_preview_http_contract_rejects_paths_extras_and_coercion(self):
        """HTTP preview has no filesystem authority from client JSON."""
        rejected_payloads = (
            {"request_id": 100, "path": "/tmp/candidate"},
            {"download_log_id": "1"},
            {"values": {"min_bitrate": "320"}},
            {"values": {"min_bitrate": 320}, "force": True},
        )
        for payload in rejected_payloads:
            with self.subTest(payload=payload):
                status, data = self._post("/api/import-preview", payload)
                self.assertEqual(status, 400)
                self.assertIn("error", data)

    @patch("web.routes.imports.cleanup_all_wrong_matches")
    def test_wrong_match_triage_starts_background_sweep(self, mock_cleanup):
        from lib.wrong_match_cleanup_service import WrongMatchCleanupSummary
        runner = _fresh_triage_runner(self)
        mock_cleanup.return_value = WrongMatchCleanupSummary(
            processed=2,
            deleted=1,
            kept_uncertain=1,
        )
        status, data = self._post("/api/wrong-matches/triage", {
            "confirm_all_wrong_matches": True,
        })

        # Issue: bulk triage must not hold the single server thread — the
        # POST returns immediately and the sweep runs on a background thread.
        self.assertEqual(status, 202)
        self.assertEqual(data["status"], "started")
        self.assertEqual(data["state"], "running")

        runner.join(timeout=5)
        mock_cleanup.assert_called_once_with(
            self.db,
            confirm_all_wrong_matches=True,
            cancellation_token=ANY,
        )

        status, data = self._get("/api/wrong-matches/triage/status")
        self.assertEqual(status, 200)
        _assert_required_fields(
            self, data, self.WRONG_MATCH_TRIAGE_STATUS_REQUIRED_FIELDS,
            "wrong match triage status response")
        self.assertEqual(data["state"], "completed")
        self.assertIsNone(data["error"])
        _assert_required_fields(
            self, data["summary"],
            self.WRONG_MATCH_TRIAGE_SUMMARY_REQUIRED_FIELDS,
            "wrong match triage summary")
        self.assertEqual(data["summary"]["processed"], 2)
        self.assertEqual(data["summary"]["deleted"], 1)

    @patch("web.routes.imports.cleanup_all_wrong_matches")
    def test_wrong_match_triage_rejects_concurrent_sweep(self, mock_cleanup):

        from lib.wrong_match_cleanup_service import WrongMatchCleanupSummary
        runner = _fresh_triage_runner(self)
        release = threading.Event()
        entered = threading.Event()

        def slow_cleanup(db, *, confirm_all_wrong_matches, cancellation_token=None):
            entered.set()
            release.wait(timeout=5)
            return WrongMatchCleanupSummary(processed=0)

        mock_cleanup.side_effect = slow_cleanup

        status, data = self._post("/api/wrong-matches/triage", {
            "confirm_all_wrong_matches": True,
        })
        self.assertEqual(status, 202)
        self.assertTrue(entered.wait(timeout=5))

        status, data = self._post("/api/wrong-matches/triage", {
            "confirm_all_wrong_matches": True,
        })
        self.assertEqual(status, 409)
        self.assertIn("already running", data["error"])

        status, data = self._get("/api/wrong-matches/triage/status")
        self.assertEqual(status, 200)
        self.assertEqual(data["state"], "running")
        self.assertIsNone(data["summary"])

        release.set()
        runner.join(timeout=5)

    def test_wrong_match_triage_status_idle_contract(self):
        _fresh_triage_runner(self)
        status, data = self._get("/api/wrong-matches/triage/status")

        self.assertEqual(status, 200)
        _assert_required_fields(
            self, data, self.WRONG_MATCH_TRIAGE_STATUS_REQUIRED_FIELDS,
            "wrong match triage status response")
        self.assertEqual(data["state"], "idle")
        self.assertIsNone(data["summary"])
        self.assertIsNone(data["error"])

    @patch("web.routes.imports.cleanup_all_wrong_matches")
    def test_wrong_match_triage_requires_full_queue_confirmation(self, mock_cleanup):
        _fresh_triage_runner(self)
        status, data = self._post("/api/wrong-matches/triage", {})

        self.assertEqual(status, 400)
        self.assertIn("confirm_all_wrong_matches", data.get("message") or data.get("error") or "")
        mock_cleanup.assert_not_called()

    def _enqueue_force_job(self) -> int:
        from lib.import_queue import force_import_dedupe_key
        log_id = self.db.log_download(
            100, outcome="rejected", soulseek_username="baduser",
            validation_result={"failed_path": "/tmp/Test Album"},
        )
        job = self.db.enqueue_import_job(
            "force_import", request_id=100,
            dedupe_key=force_import_dedupe_key(log_id),
            payload={"download_log_id": 1, "failed_path": "/tmp/Test Album"},
            message="Import queued",
        )
        return job.id

    def test_import_jobs_contract(self):
        self._enqueue_force_job()
        status, data = self._get("/api/import-jobs")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, {"jobs", "counts"}, "import jobs response")
        _assert_required_fields(self, data["jobs"][0], self.IMPORT_JOB_REQUIRED_FIELDS,
                                "import jobs item")

    def test_import_job_detail_contract(self):
        job_id = self._enqueue_force_job()
        status, data = self._get(f"/api/import-jobs/{job_id}")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, {"job"}, "import job detail response")
        _assert_required_fields(self, data["job"], self.IMPORT_JOB_REQUIRED_FIELDS,
                                "import job detail")

    def test_automation_recovery_detail_uses_shared_typed_projection(self):
        request_id = 405
        self.db.seed_request(make_request_row(
            id=request_id,
            status="wanted",
            artist_name="Recovery Artist",
            album_title="Recovery Album",
            mb_release_id="75dbf62e-7dd2-4ddc-b57b-9bad1758b6b0",
        ))
        job = handoff_automation_owner(
            self.db,
            request_id,
            canonical_path="/processing/recovery-album",
        )
        # The cleanup-journal fake lands with the terminal slice.  This
        # instance seam pins the read adapter's real missing-journal mapping.
        with patch.object(
            self.db,
            "get_processing_cleanup_journal",
            lambda *, request_id, job_id: None,
            create=True,
        ):
            status, data = self._get(
                f"/api/import-jobs/{job.id}/recovery"
            )

        self.assertEqual(status, 200)
        self.assertEqual(data["outcome"], "ok")
        detail = data["detail"]
        self.assertEqual(detail["owner_stage"]["job_id"], job.id)
        self.assertTrue(detail["owner_stage"]["exact_active_owner"])
        self.assertEqual(
            detail["canonical_path"],
            "/processing/recovery-album",
        )
        self.assertEqual(detail["execution_liveness"]["status"], "dead")
        self.assertEqual(detail["completion"]["status"], "absent")
        self.assertEqual(detail["exact_library"]["status"], "missing")
        self.assertEqual(detail["cleanup_journal"]["status"], "missing")

    def test_automation_recovery_detail_not_found_is_typed(self):
        status, data = self._get("/api/import-jobs/999999/recovery")

        self.assertEqual(status, 404)
        self.assertEqual(data["outcome"], "not_found")
        self.assertIsNone(data["detail"])

    def test_automation_recovery_post_route_is_absent(self):
        status, data = self._post("/api/import-jobs/999999/recovery", {
            "action": "retry",
            "reason": "missing operation",
        })

        self.assertEqual(status, 404)
        self.assertIn("error", data)

    def test_import_jobs_timeline_contract(self):
        self._enqueue_force_job()
        status, data = self._get("/api/import-jobs/timeline")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, {"jobs", "counts"},
                                "import jobs timeline response")
        _assert_required_fields(self, data["jobs"][0], self.IMPORT_JOB_REQUIRED_FIELDS,
                                "import jobs timeline item")
        _assert_required_fields(self, data["jobs"][0], {"artist_name", "album_title"},
                                "import jobs timeline identity")
        _assert_required_fields(
            self,
            data["jobs"][0],
            {"badge", "badge_class", "border_color", "summary"},
            "server-classified import job display",
        )
        self.assertEqual(data["jobs"][0]["badge"], "Waiting preview")
        # The identity join resolved through the seeded request row.
        self.assertEqual(data["jobs"][0]["artist_name"],
                         self.db.request(100)["artist_name"])

    def test_import_jobs_timeline_surfaces_recovery_as_environment_failure(self):
        job_id = self._enqueue_force_job()
        row = next(row for row in self.db._import_jobs if row["id"] == job_id)
        row.update(status="recovery_required", message="exact owner replay failed")

        status, data = self._get("/api/import-jobs/timeline")

        self.assertEqual(status, 200)
        item = data["jobs"][0]
        self.assertEqual(item["badge"], "Recovery required")
        self.assertEqual(item["badge_class"], "badge-warn")
        self.assertEqual(item["border_color"], "#a86f20")
        self.assertEqual(item["summary"], "exact owner replay failed")

    def test_import_jobs_timeline_caps_at_50(self):
        """The route hardcodes limit=50 — seed 51 jobs, count the page."""
        for i in range(51):
            self.db.enqueue_import_job(
                "force_import", request_id=100,
                dedupe_key=f"force_import:download_log:{i}",
                payload={"download_log_id": 1, "failed_path": f"/tmp/a{i}"},
            )
        status, data = self._get("/api/import-jobs/timeline")
        self.assertEqual(status, 200)
        self.assertEqual(len(data["jobs"]), 50)

    def test_import_jobs_rejects_invalid_filters(self):
        status, data = self._get("/api/import-jobs?status=bad")
        self.assertEqual(status, 400)
        self.assertIn("error", data)

        status, data = self._get("/api/import-jobs?request_id=abc")
        self.assertEqual(status, 400)
        self.assertIn("error", data)

if __name__ == "__main__":
    unittest.main()
