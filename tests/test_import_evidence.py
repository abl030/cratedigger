"""Tests for action-time import evidence acquisition."""

from __future__ import annotations

import os
import shutil
import tempfile
import unittest
from unittest.mock import patch

from lib.beets_db import AlbumInfo
from lib.import_evidence import (
    ActionEvidenceProvenance,
    CurrentEvidenceActionResult,
    ensure_candidate_evidence_for_action,
    ensure_current_evidence_for_action,
    load_current_evidence_for_action,
)
from lib.quality import (
    AlbumQualityEvidence,
    AudioQualityMeasurement,
    QualityRankConfig,
)
from lib.quality_evidence import (
    EvidenceBuildResult,
    snapshot_audio_files,
)
from tests.fakes import FakePipelineDB
from tests.helpers import make_album_quality_evidence, make_request_row


class TestImportEvidenceAcquisition(unittest.TestCase):
    def setUp(self) -> None:
        self.root = tempfile.mkdtemp()
        with open(os.path.join(self.root, "01 - Track.mp3"), "wb") as handle:
            handle.write(b"audio")
        self.db = FakePipelineDB()
        self.db.seed_request(make_request_row(id=42, mb_release_id="release-1"))
        self.download_log_id = self.db.log_download(
            request_id=42,
            outcome="rejected",
        )

    def tearDown(self) -> None:
        shutil.rmtree(self.root, ignore_errors=True)

    def _candidate_evidence(self) -> AlbumQualityEvidence:
        return make_album_quality_evidence(
            mb_release_id="release-1",
            files=snapshot_audio_files(self.root),
        )

    def _persist_candidate(self) -> int:
        evidence = self._candidate_evidence()
        self.db.upsert_album_quality_evidence(evidence)
        persisted = self.db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert persisted is not None and persisted.id is not None
        self.db.set_download_log_candidate_evidence(
            self.download_log_id, persisted.id
        )
        return persisted.id

    def _persist_current(self) -> int:
        evidence = make_album_quality_evidence(
            mb_release_id="release-1",
            files=snapshot_audio_files(self.root),
        )
        self.db.upsert_album_quality_evidence(evidence)
        persisted = self.db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert persisted is not None and persisted.id is not None
        self.db.set_request_current_evidence(42, persisted.id)
        return persisted.id

    def _persist_lossless_transcode_current_without_v0(self) -> int:
        stale_evidence = make_album_quality_evidence(
            mb_release_id="release-1",
            files=snapshot_audio_files(self.root),
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=108,
                avg_bitrate_kbps=114,
                median_bitrate_kbps=114,
                format="Opus",
                is_cbr=False,
                spectral_grade=None,
                spectral_bitrate_kbps=None,
                verified_lossless=False,
                was_converted_from="flac",
            ),
            v0_metric=None,
            codec="opus",
            container="opus",
            storage_format="Opus",
        )
        self.db.upsert_album_quality_evidence(stale_evidence)
        persisted = self.db.find_album_quality_evidence(
            mb_release_id=stale_evidence.mb_release_id,
            snapshot_fingerprint=stale_evidence.snapshot_fingerprint,
        )
        assert persisted is not None and persisted.id is not None
        self.db.set_request_current_evidence(42, persisted.id)
        return persisted.id

    def test_reused_candidate_evidence_is_action_ready(self):
        self._persist_candidate()

        result = ensure_candidate_evidence_for_action(
            self.db,
            source_path=self.root,
            download_log_id=self.download_log_id,
        )

        self.assertTrue(result.available)
        self.assertEqual(result.provenance.candidate_status, "reused")
        self.assertEqual(result.provenance.snapshot_guard, "matched")
        self.assertFalse(result.provenance.fail_closed)

    def test_stale_candidate_snapshot_fails_closed(self):
        self._persist_candidate()
        with open(os.path.join(self.root, "01 - Track.mp3"), "ab") as handle:
            handle.write(b" changed")

        result = ensure_candidate_evidence_for_action(
            self.db,
            source_path=self.root,
            download_log_id=self.download_log_id,
        )

        self.assertFalse(result.available)
        self.assertIsNone(result.evidence)
        self.assertEqual(result.provenance.candidate_status, "stale")
        self.assertEqual(result.provenance.snapshot_guard, "stale")
        self.assertTrue(result.provenance.fail_closed)

    def test_missing_candidate_evidence_returns_fail_closed_provenance(self):
        result = ensure_candidate_evidence_for_action(
            self.db,
            source_path=self.root,
            download_log_id=self.download_log_id,
        )

        self.assertFalse(result.available)
        self.assertIsNone(result.evidence)
        self.assertEqual(result.provenance.candidate_status, "missing")
        self.assertIn("no candidate evidence found", result.provenance.fallback_reason or "")
        self.assertTrue(result.provenance.fail_closed)

    def test_matching_current_evidence_loads_without_backfill(self):
        self._persist_current()

        def backfill(*_args, **_kwargs):
            raise AssertionError("backfill should not be called")

        result = ensure_current_evidence_for_action(
            self.db,
            request_id=42,
            mb_release_id="release-1",
            current_album_path=self.root,
            backfill_builder=backfill,
        )

        self.assertTrue(result.available)
        self.assertEqual(result.provenance.current_status, "loaded")
        self.assertEqual(result.provenance.snapshot_guard, "matched")

    def test_missing_current_evidence_backfills_from_album_info(self):
        result = ensure_current_evidence_for_action(
            self.db,
            request_id=42,
            mb_release_id="release-1",
            album_info=AlbumInfo(
                album_id=1,
                track_count=1,
                min_bitrate_kbps=240,
                avg_bitrate_kbps=250,
                median_bitrate_kbps=245,
                is_cbr=False,
                album_path=self.root,
                format="MP3",
            ),
        )

        self.assertTrue(result.available)
        self.assertEqual(result.provenance.current_status, "backfilled")
        # The FK is wired by the backfill production code.
        evidence_id = self.db.get_request_current_evidence_id(42)
        self.assertIsNotNone(evidence_id)
        loaded = self.db.load_album_quality_evidence_by_id(evidence_id)
        self.assertIsNotNone(loaded)

    def test_stale_current_evidence_backfills_from_album_info(self):
        self._persist_current()
        with open(os.path.join(self.root, "01 - Track.mp3"), "ab") as handle:
            handle.write(b" changed")

        result = ensure_current_evidence_for_action(
            self.db,
            request_id=42,
            mb_release_id="release-1",
            current_album_path=self.root,
            album_info=AlbumInfo(
                album_id=1,
                track_count=1,
                min_bitrate_kbps=230,
                avg_bitrate_kbps=240,
                median_bitrate_kbps=235,
                is_cbr=False,
                album_path=self.root,
                format="MP3",
            ),
        )

        self.assertTrue(result.available)
        self.assertEqual(result.provenance.current_status, "backfilled")
        self.assertEqual(
            result.provenance.fallback_reason,
            "current album files changed since evidence capture",
        )
        assert result.evidence is not None
        self.assertEqual(result.evidence.measurement.min_bitrate_kbps, 230)

    def test_lossless_transcode_current_evidence_missing_v0_backfills(self):
        self.db.update_request_fields(
            42,
            current_spectral_grade="likely_transcode",
            current_spectral_bitrate=128,
            current_lossless_source_v0_probe_min_bitrate=189,
            current_lossless_source_v0_probe_avg_bitrate=195,
            current_lossless_source_v0_probe_median_bitrate=195,
        )
        self._persist_lossless_transcode_current_without_v0()

        result = ensure_current_evidence_for_action(
            self.db,
            request_id=42,
            mb_release_id="release-1",
            current_album_path=self.root,
            album_info=AlbumInfo(
                album_id=1,
                track_count=1,
                min_bitrate_kbps=108,
                avg_bitrate_kbps=114,
                median_bitrate_kbps=114,
                is_cbr=False,
                album_path=self.root,
                format="Opus",
            ),
        )

        self.assertTrue(result.available)
        self.assertEqual(result.provenance.current_status, "backfilled")
        self.assertIn(
            "lossless-source V0 metric is required",
            result.provenance.fallback_reason or "",
        )
        assert result.evidence is not None
        assert result.evidence.v0_metric is not None
        self.assertEqual(result.evidence.v0_metric.avg_bitrate_kbps, 195)

    def test_lossless_transcode_current_evidence_missing_v0_fails_closed(self):
        stale_evidence_id = self._persist_lossless_transcode_current_without_v0()

        result = ensure_current_evidence_for_action(
            self.db,
            request_id=42,
            mb_release_id="release-1",
            current_album_path=self.root,
            album_info=AlbumInfo(
                album_id=1,
                track_count=1,
                min_bitrate_kbps=108,
                avg_bitrate_kbps=114,
                median_bitrate_kbps=114,
                is_cbr=False,
                album_path=self.root,
                format="Opus",
            ),
        )

        self.assertFalse(result.available)
        self.assertEqual(result.provenance.snapshot_guard, "matched")
        self.assertIn(
            "lossless-source V0 metric is required",
            result.provenance.fallback_reason or "",
        )
        self.assertEqual(
            self.db.get_request_current_evidence_id(42),
            stale_evidence_id,
        )

    def test_stale_current_evidence_is_not_reused_as_preloaded_backfill(self):
        self._persist_current()
        with open(os.path.join(self.root, "01 - Track.mp3"), "ab") as handle:
            handle.write(b" changed")

        with patch(
            "lib.import_evidence.load_or_backfill_current_evidence",
            return_value=EvidenceBuildResult(None, "empty_current", "album not in beets"),
        ) as backfill:
            result = ensure_current_evidence_for_action(
                self.db,
                request_id=42,
                mb_release_id="release-1",
                current_album_path=self.root,
            )

        self.assertFalse(result.available)
        self.assertEqual(result.provenance.current_status, "missing")
        self.assertIsNone(backfill.call_args.kwargs["preloaded_evidence"])
        self.assertTrue(backfill.call_args.kwargs["preloaded"])


class TestLoadCurrentEvidenceForAction(unittest.TestCase):
    def setUp(self) -> None:
        self.root = tempfile.mkdtemp()
        self.db = FakePipelineDB()
        self.db.seed_request(make_request_row(id=42, mb_release_id="release-1"))
        self.album_info = AlbumInfo(
            album_id=1,
            track_count=1,
            min_bitrate_kbps=240,
            avg_bitrate_kbps=250,
            median_bitrate_kbps=245,
            is_cbr=False,
            album_path=self.root,
            format="MP3",
        )

    def tearDown(self) -> None:
        shutil.rmtree(self.root, ignore_errors=True)

    def _available_result(self) -> CurrentEvidenceActionResult:
        evidence = make_album_quality_evidence(mb_release_id="release-1")
        return CurrentEvidenceActionResult(
            evidence=evidence,
            provenance=ActionEvidenceProvenance(
                current_status="loaded",
                snapshot_guard="matched",
            ),
        )

    def test_happy_path_returns_ensure_result_unchanged(self):
        expected = self._available_result()
        with patch("lib.beets_db.BeetsDB") as beets_cls, patch(
            "lib.import_evidence.ensure_current_evidence_for_action",
            return_value=expected,
        ) as ensure:
            beets_cls.return_value.__enter__.return_value.get_album_info.return_value = (
                self.album_info
            )

            result = load_current_evidence_for_action(
                self.db,
                request_id=42,
                mb_release_id="release-1",
                quality_ranks=QualityRankConfig.defaults(),
                beets_library_root="/tmp/beets",
            )

        self.assertIs(result, expected)
        ensure.assert_called_once()
        kwargs = ensure.call_args.kwargs
        self.assertEqual(kwargs["request_id"], 42)
        self.assertEqual(kwargs["mb_release_id"], "release-1")
        self.assertIs(kwargs["album_info"], self.album_info)
        self.assertEqual(kwargs["current_album_path"], self.root)
        self.assertEqual(kwargs["beets_library_root"], "/tmp/beets")

    def test_beets_absent_returns_none(self):
        with patch("lib.beets_db.BeetsDB") as beets_cls, patch(
            "lib.import_evidence.ensure_current_evidence_for_action"
        ) as ensure:
            beets_cls.return_value.__enter__.return_value.get_album_info.return_value = None

            result = load_current_evidence_for_action(
                self.db,
                request_id=42,
                mb_release_id="release-1",
            )

        self.assertIsNone(result)
        ensure.assert_not_called()

    def test_ensure_raises_returns_fail_closed_result(self):
        with patch("lib.beets_db.BeetsDB") as beets_cls, patch(
            "lib.import_evidence.ensure_current_evidence_for_action",
            side_effect=RuntimeError("backfill failed"),
        ):
            beets_cls.return_value.__enter__.return_value.get_album_info.return_value = (
                self.album_info
            )

            result = load_current_evidence_for_action(
                self.db,
                request_id=42,
                mb_release_id="release-1",
            )

        self.assertIsNotNone(result)
        assert result is not None
        self.assertIsNone(result.evidence)
        self.assertTrue(result.provenance.fail_closed)
        self.assertEqual(result.provenance.current_status, "failed")
        self.assertIn("RuntimeError", result.provenance.fallback_reason or "")
        self.assertIn("backfill failed", result.provenance.fallback_reason or "")

    def test_unavailable_fail_closed_passes_through(self):
        fail_closed = CurrentEvidenceActionResult(
            evidence=None,
            provenance=ActionEvidenceProvenance(
                current_status="failed",
                fallback_reason="snapshot drift",
                fail_closed=True,
            ),
        )
        with patch("lib.beets_db.BeetsDB") as beets_cls, patch(
            "lib.import_evidence.ensure_current_evidence_for_action",
            return_value=fail_closed,
        ):
            beets_cls.return_value.__enter__.return_value.get_album_info.return_value = (
                self.album_info
            )

            result = load_current_evidence_for_action(
                self.db,
                request_id=42,
                mb_release_id="release-1",
            )

        self.assertIs(result, fail_closed)

    def test_default_quality_ranks_resolves_to_defaults(self):
        with patch("lib.beets_db.BeetsDB") as beets_cls, patch(
            "lib.import_evidence.ensure_current_evidence_for_action",
            return_value=self._available_result(),
        ) as ensure:
            get_album_info = beets_cls.return_value.__enter__.return_value.get_album_info
            get_album_info.return_value = self.album_info

            load_current_evidence_for_action(
                self.db,
                request_id=42,
                mb_release_id="release-1",
            )

        get_album_info.assert_called_once()
        passed_cfg = get_album_info.call_args.args[1]
        self.assertEqual(passed_cfg, QualityRankConfig.defaults())
        self.assertEqual(ensure.call_args.kwargs["quality_ranks"], QualityRankConfig.defaults())


if __name__ == "__main__":
    unittest.main()
