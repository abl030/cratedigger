#!/usr/bin/env python3
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

from datetime import datetime, timezone
import os
import sys
import threading
import unittest
from unittest.mock import patch

import msgspec
from web.classify import ClassifiedEntry

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from tests.web._harness import (
    _assert_required_fields,
    _FakeDbWebServerCase,
    _fresh_triage_runner,
)

from tests.fakes import FakeBeetsDB
from tests.helpers import make_request_row


class TestPipelineRouteContracts(_FakeDbWebServerCase):
    """Contract tests for frontend-consumed pipeline GET routes."""

    PIPELINE_ITEM_REQUIRED_FIELDS = {
        "id", "artist_name", "album_title", "year", "format", "country",
        "source", "created_at", "status", "search_attempts",
        "download_attempts", "validation_attempts", "beets_distance",
        "mb_release_id",
        # Release-group id surfaces so the pipeline-row Replace button
        # (R7) can render — both the standard-mode source label and
        # the picker's inverted-row sibling lookup need it.
        "mb_release_group_id",
        "imported_path", "current_spectral_bitrate",
        "last_download_spectral_bitrate", "current_spectral_grade",
        "last_download_spectral_grade", "verified_lossless",
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
        "disambiguation_failure", "disambiguation_detail", "bad_extensions",
        "spectral_grade", "spectral_bitrate", "existing_min_bitrate",
        "existing_spectral_grade", "existing_spectral_bitrate", "existing_format",
        "source", "youtube_metadata",
        "wrong_match_triage_action", "wrong_match_triage_summary",
        "wrong_match_triage_reason", "wrong_match_triage_preview_verdict",
        "wrong_match_triage_preview_decision",
        "wrong_match_triage_stage_chain", "wrong_match_triage_detail",
    } | {field.name for field in msgspec.structs.fields(ClassifiedEntry)}
    STATUS_WANTED_REQUIRED_FIELDS = {
        "id", "artist", "album", "mb_release_id", "source", "created_at",
    }
    IMPORT_PREVIEW_REQUIRED_FIELDS = {
        "mode", "verdict", "would_import", "confident_reject", "uncertain",
        "cleanup_eligible", "decision", "reason", "stage_chain",
    }
    WRONG_MATCH_TRIAGE_SUMMARY_REQUIRED_FIELDS = {
        "processed", "deleted", "deleted_verified_lossless_parent",
        "kept_would_import", "kept_uncertain",
        "skipped_candidate_evidence_missing", "skipped_candidate_evidence_stale",
        "skipped_current_evidence_missing", "skipped_current_evidence_stale",
        "skipped_current_evidence_failed",
        "skipped_active_job", "skipped_invalid_row", "skipped_missing_path",
        "skipped_operational", "delete_failed", "results",
    }
    WRONG_MATCH_TRIAGE_STATUS_REQUIRED_FIELDS = {
        "state", "started_at", "finished_at", "summary", "error",
    }
    IMPORT_JOB_REQUIRED_FIELDS = {
        "id", "job_type", "status", "request_id", "dedupe_key", "payload",
        "result", "message", "error", "attempts", "worker_id", "created_at",
        "updated_at", "started_at", "heartbeat_at", "completed_at", "deduped",
        "preview_status", "preview_result", "preview_message", "preview_error",
        "preview_attempts", "preview_worker_id", "preview_started_at",
        "preview_heartbeat_at", "preview_completed_at", "importable_at",
    }
    DISK_COVERAGE_COUNT_FIELDS = {
        "active_total", "on_disk_total", "off_disk_total", "by_status",
        "on_disk_by_status", "off_disk_by_status", "inverse_total",
    }
    DISK_COVERAGE_ROW_FIELDS = {
        "id", "status", "artist_name", "album_title", "mb_release_id",
        "discogs_release_id",
    }
    DISK_COVERAGE_INVERSE_FIELDS = {
        "id", "album", "albumartist", "mb_albumid", "discogs_albumid",
    }

    def setUp(self) -> None:
        super().setUp()
        # The detail/log fixtures: one imported request with a track and
        # a real success download row, plus one wanted request.
        self.db.seed_request(make_request_row(
            id=100, status="imported", min_bitrate=320,
            imported_path="/mnt/virtio/Music/Beets/Test",
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
            ),
            v0_metric=AlbumQualityV0Metric(
                min_bitrate_kbps=246,
                avg_bitrate_kbps=258,
                median_bitrate_kbps=257,
                source_lineage="lossless_source",
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
        self.assertEqual(item["badge"], "Triaged · kept")
        self.assertEqual(item["badge_class"], "badge-warn")
        self.assertEqual(item["border_color"], "#a33")
        self.assertEqual(item["existing_format"], "Opus")
        self.assertEqual(item["existing_min_bitrate"], 118)
        self.assertEqual(item["existing_avg_bitrate"], 124)
        self.assertEqual(item["existing_median_bitrate"], 122)
        self.assertEqual(item["existing_spectral_grade"], "likely_transcode")
        self.assertEqual(item["existing_spectral_bitrate"], 96)
        self.assertEqual(item["existing_v0_probe_kind"], "lossless_source")
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
            ),
            v0_metric=AlbumQualityV0Metric(
                min_bitrate_kbps=193,
                avg_bitrate_kbps=256,
                median_bitrate_kbps=258,
                source_lineage="lossless_source",
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
        self.assertEqual(item["badge"], "Triaged · deleted")
        self.assertEqual(item["existing_format"], "Opus")
        self.assertEqual(item["existing_min_bitrate"], 93)
        self.assertEqual(item["existing_avg_bitrate"], 129)
        self.assertEqual(item["existing_median_bitrate"], 128)
        self.assertEqual(item["existing_spectral_grade"], "suspect")
        self.assertEqual(item["existing_spectral_bitrate"], 96)
        self.assertEqual(item["existing_v0_probe_kind"], "lossless_source")
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
                source_lineage="on_disk_research",
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
        self.assertEqual(item["badge"], "Triaged · deleted")
        self.assertEqual(item["existing_format"], "MP3")
        self.assertEqual(item["existing_min_bitrate"], 320)
        self.assertEqual(item["existing_avg_bitrate"], 320)
        self.assertEqual(item["existing_median_bitrate"], 320)
        self.assertEqual(item["existing_spectral_grade"], "genuine")
        self.assertEqual(item["existing_spectral_bitrate"], 96)
        self.assertEqual(
            item["existing_v0_probe_kind"], "on_disk_research"
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
            mb_release_id="pipeline-log-overlay",
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
                source_lineage="native_lossy_research",
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
        self.assertEqual(item["badge"], "Triaged · deleted")
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

    def test_disk_coverage_contract(self):
        import web.server as srv

        self.db.seed_request(make_request_row(
            id=9001, status="wanted", mb_release_id="disk-missing-mbid",
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
            self, data["off_disk"][0], self.DISK_COVERAGE_ROW_FIELDS,
            "disk coverage off-disk row")

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
                         "Wrong match (dist 0.190) · moundsofass")
        self.assertEqual(item["badge"], "Triaged · deleted")
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
        _assert_required_fields(self, data, {"counts", "wanted", "downloading", "imported", "manual",
                                             "imported_total", "imported_truncated"},
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
        base = datetime(2026, 5, 1, tzinfo=timezone.utc)
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

    SEARCH_REQUIRED_FIELDS = {"query", "items", "total"}

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

    DETAIL_RESPONSE_REQUIRED_FIELDS = {
        "request", "history", "tracks", "manual_reason", "last_search",
    }
    LAST_SEARCH_REQUIRED_FIELDS = {
        "variant", "final_state", "outcome", "top_candidates",
    }
    CANDIDATE_SCORE_REQUIRED_FIELDS = {
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
        _assert_required_fields(self, data["history"][0], self.HISTORY_REQUIRED_FIELDS,
                                "pipeline detail history item")
        # Default mock state: no search history → last_search is None and
        # manual_reason is None. Both keys are still present.
        self.assertIsNone(data["last_search"])
        self.assertIsNone(data["manual_reason"])

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

    def test_pipeline_detail_surfaces_manual_reason(self):
        """manual_reason='search_exhausted' is exposed on the detail response."""
        row = self.db.get_request(100)
        assert row is not None
        self.db.seed_request({
            **row,
            "status": "manual",
            "manual_reason": "search_exhausted",
        })
        status, data = self._get("/api/pipeline/100")

        self.assertEqual(status, 200)
        self.assertEqual(data["manual_reason"], "search_exhausted")

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
        self.assertEqual(item["badge"], "Triaged · deleted")
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
        import threading

        from lib.wrong_match_cleanup_service import WrongMatchCleanupSummary
        runner = _fresh_triage_runner(self)
        release = threading.Event()
        entered = threading.Event()

        def slow_cleanup(db, *, confirm_all_wrong_matches):
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
            payload={"failed_path": "/tmp/Test Album"},
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

    def test_import_jobs_timeline_caps_at_50(self):
        """The route hardcodes limit=50 — seed 51 jobs, count the page."""
        for i in range(51):
            self.db.enqueue_import_job(
                "force_import", request_id=100,
                dedupe_key=f"force_import:download_log:{i}",
                payload={"failed_path": f"/tmp/a{i}"},
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
