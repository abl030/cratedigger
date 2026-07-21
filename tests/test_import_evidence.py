"""Tests for action-time import evidence acquisition."""

from __future__ import annotations

import os
import shutil
import tempfile
import unittest
from unittest.mock import patch

from lib.beets_db import (
    AlbumInfo,
    CurrentBeetsItem,
    CurrentBeetsUnique,
    release_identity_for_lookup,
)
from lib.import_evidence import (
    ActionEvidenceProvenance,
    CurrentEvidenceActionResult,
    ensure_candidate_evidence_for_action,
    ensure_current_evidence_for_action,
    load_current_evidence_for_action,
)
from lib.quality import (
    AlbumQualityEvidence,
    AlbumQualityV0Metric,
    AudioQualityMeasurement,
    QualityRankConfig,
)
from lib.quality_evidence import (
    EvidenceBuildResult,
    snapshot_audio_files,
)
from tests.fakes import FakeBeetsDB, FakePipelineDB
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
            source_path=self.root,
            files=snapshot_audio_files(self.root),
        )

    def _current_release(self) -> CurrentBeetsUnique:
        identity = release_identity_for_lookup("release-1")
        assert identity is not None
        return CurrentBeetsUnique(
            identity=identity,
            album_id=1,
            album_path=self.root,
            items=(CurrentBeetsItem(
                id=1,
                path=os.path.join(self.root, "01 - Track.mp3"),
                format="MP3",
                bitrate=250_000,
            ),),
            selectors=("mb_albumid:release-1",),
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
            source_path=self.root,
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

    def test_matching_candidate_keeps_capture_path_at_moved_action_path(self):
        """Same bytes may move without rewriting capture-time evidence."""

        evidence = make_album_quality_evidence(
            mb_release_id="release-1",
            source_path="/pre-quarantine/Artist - Album",
            files=snapshot_audio_files(self.root),
        )
        self.db.upsert_album_quality_evidence(evidence)
        persisted = self.db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert persisted is not None and persisted.id is not None
        self.db.set_download_log_candidate_evidence(
            self.download_log_id,
            persisted.id,
        )

        result = ensure_candidate_evidence_for_action(
            self.db,
            source_path=self.root,
            download_log_id=self.download_log_id,
        )

        self.assertTrue(result.available)
        assert result.evidence is not None
        self.assertEqual(
            result.evidence.source_path,
            "/pre-quarantine/Artist - Album",
        )
        unchanged = self.db.load_album_quality_evidence_by_id(persisted.id)
        assert unchanged is not None
        self.assertEqual(
            unchanged.source_path,
            "/pre-quarantine/Artist - Album",
        )

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
            current_release=self._current_release(),
            backfill_builder=backfill,
        )

        self.assertTrue(result.available)
        self.assertEqual(result.provenance.current_status, "loaded")
        self.assertEqual(result.provenance.snapshot_guard, "matched")

    def test_matching_candidate_capture_path_remains_historical(self):
        evidence = make_album_quality_evidence(
            mb_release_id="release-1",
            source_path="/tmp/disposable-candidate",
            files=snapshot_audio_files(self.root),
        )
        self.db.upsert_album_quality_evidence(evidence)
        persisted = self.db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert persisted is not None and persisted.id is not None
        self.db.set_request_current_evidence(42, persisted.id)

        def backfill(*_args, **_kwargs):
            raise AssertionError("matching content must not be remeasured")

        result = ensure_current_evidence_for_action(
            self.db,
            request_id=42,
            mb_release_id="release-1",
            current_release=self._current_release(),
            backfill_builder=backfill,
        )

        self.assertTrue(result.available)
        self.assertEqual(result.provenance.current_status, "loaded")
        assert result.evidence is not None
        self.assertEqual(result.evidence.source_path, "/tmp/disposable-candidate")
        self.assertEqual(result.provenance.installed_path, self.root)
        linked = self.db.load_album_quality_evidence_by_id(persisted.id)
        assert linked is not None
        self.assertEqual(linked.source_path, "/tmp/disposable-candidate")

    def test_matching_v1_current_evidence_rebuilds_as_v3(self):
        evidence = make_album_quality_evidence(
            mb_release_id="release-1",
            files=snapshot_audio_files(self.root),
            lineage_version=1,
        )
        self.db.upsert_album_quality_evidence(evidence)
        persisted = self.db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert persisted is not None and persisted.id is not None
        self.db.set_request_current_evidence(42, persisted.id)

        result = ensure_current_evidence_for_action(
            self.db,
            request_id=42,
            mb_release_id="release-1",
            current_release=self._current_release(),
            album_info=AlbumInfo(
                album_id=1,
                track_count=1,
                min_bitrate_kbps=256,
                avg_bitrate_kbps=256,
                median_bitrate_kbps=256,
                is_cbr=True,
                album_path=self.root,
                format="AAC",
            ),
        )

        self.assertTrue(result.available)
        self.assertEqual(result.provenance.current_status, "backfilled")
        self.assertIn("lineage_version", result.provenance.fallback_reason or "")
        assert result.evidence is not None
        self.assertEqual(result.evidence.lineage_version, 4)
        self.assertEqual(result.evidence.measurement.format, "AAC")
        self.assertEqual(result.evidence.measurement.avg_bitrate_kbps, 256)
        self.assertEqual(
            self.db.get_request_current_evidence_id(42),
            persisted.id,
        )

    def test_v1_lossless_transcode_rebuild_preserves_only_source_v0_metric(self):
        self.db.update_request_fields(
            42,
            current_spectral_grade="genuine",
            current_spectral_bitrate=None,
            current_lossless_source_v0_probe_min_bitrate=211,
            current_lossless_source_v0_probe_avg_bitrate=222,
            current_lossless_source_v0_probe_median_bitrate=220,
        )
        evidence = make_album_quality_evidence(
            mb_release_id="release-1",
            files=snapshot_audio_files(self.root),
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=108,
                avg_bitrate_kbps=114,
                median_bitrate_kbps=114,
                format="Opus",
                is_cbr=False,
                spectral_grade="likely_transcode",
                spectral_bitrate_kbps=96,
                was_converted_from="flac",
            ),
            lineage_version=1,
            v0_metric=AlbumQualityV0Metric(
                min_bitrate_kbps=189,
                avg_bitrate_kbps=195,
                median_bitrate_kbps=195,
                subject="source",
            ),
        )
        self.db.upsert_album_quality_evidence(evidence)
        persisted = self.db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert persisted is not None and persisted.id is not None
        self.db.set_request_current_evidence(42, persisted.id)

        result = ensure_current_evidence_for_action(
            self.db,
            request_id=42,
            mb_release_id="release-1",
            current_release=self._current_release(),
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
        assert result.evidence is not None
        self.assertEqual(result.evidence.lineage_version, 4)
        # A v1 spectral result has no subject metadata, so it cannot be
        # promoted as installed-HAVE authority during the rebuild.  The
        # explicitly source-subject V0 metric remains safe to carry.
        self.assertIsNone(result.evidence.measurement.spectral_grade)
        self.assertIsNone(result.evidence.measurement.spectral_bitrate_kbps)
        assert result.evidence.v0_metric is not None
        self.assertEqual(
            result.evidence.v0_metric.subject,
            "source",
        )
        self.assertEqual(result.evidence.v0_metric.avg_bitrate_kbps, 195)

    def test_missing_current_evidence_backfills_from_album_info(self):
        result = ensure_current_evidence_for_action(
            self.db,
            request_id=42,
            mb_release_id="release-1",
            current_release=self._current_release(),
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

    def test_stale_a_backfills_b_but_waits_for_exact_b_enrichment(self):
        stale_id = self._persist_current()
        with open(os.path.join(self.root, "01 - Track.mp3"), "ab") as handle:
            handle.write(b" changed")

        result = ensure_current_evidence_for_action(
            self.db,
            request_id=42,
            mb_release_id="release-1",
            current_release=self._current_release(),
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

        self.assertFalse(result.available)
        self.assertEqual(result.provenance.current_status, "failed")
        self.assertEqual(result.provenance.snapshot_guard, "stale")
        self.assertIn("spectral", result.provenance.fallback_reason or "")
        self.assertIn("V0", result.provenance.fallback_reason or "")
        linked_id = self.db.get_request_current_evidence_id(42)
        self.assertIsNotNone(linked_id)
        self.assertNotEqual(linked_id, stale_id)
        linked = self.db.load_album_quality_evidence_by_id(linked_id)
        assert linked is not None
        self.assertEqual(linked.measurement.min_bitrate_kbps, 230)
        assert linked.id is not None
        self.assertTrue(self.db.persist_current_spectral_measurement(
            request_id=42,
            expected_evidence_id=linked.id,
            expected_snapshot_fingerprint=linked.snapshot_fingerprint,
            grade="genuine",
            bitrate_kbps=230,
        ))
        self.assertTrue(self.db.claim_current_v0_research_attempt(
            request_id=42,
            expected_evidence_id=linked.id,
            expected_snapshot_fingerprint=linked.snapshot_fingerprint,
        ))

        completed = ensure_current_evidence_for_action(
            self.db,
            request_id=42,
            mb_release_id="release-1",
            current_release=self._current_release(),
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
        self.assertTrue(completed.available)
        assert completed.evidence is not None
        self.assertEqual(completed.evidence.id, linked.id)

    def test_stale_rebuild_stays_fail_closed_across_unenriched_retry(self):
        """The live #743 drift shape cannot become safe merely by retrying."""

        self._persist_current()
        with open(os.path.join(self.root, "01 - Track.mp3"), "ab") as handle:
            handle.write(b" changed")
        album_info = AlbumInfo(
            album_id=1,
            track_count=1,
            min_bitrate_kbps=230,
            avg_bitrate_kbps=240,
            median_bitrate_kbps=235,
            is_cbr=False,
            album_path=self.root,
            format="MP3",
        )

        first = ensure_current_evidence_for_action(
            self.db,
            request_id=42,
            mb_release_id="release-1",
            current_release=self._current_release(),
            album_info=album_info,
        )
        second = ensure_current_evidence_for_action(
            self.db,
            request_id=42,
            mb_release_id="release-1",
            current_release=self._current_release(),
            album_info=album_info,
        )

        self.assertFalse(first.available)
        self.assertFalse(
            second.available,
            "retry accepted a newly linked snapshot before enrichment",
        )
        self.assertIn("spectral", second.provenance.fallback_reason or "")
        self.assertIn("V0", second.provenance.fallback_reason or "")

    def test_request_v0_scalar_cannot_rescue_missing_linked_anchor(self):
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
            current_release=self._current_release(),
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
        self.assertEqual(result.provenance.current_status, "failed")
        self.assertIn(
            "lossless-source V0 metric is required",
            result.provenance.fallback_reason or "",
        )
        self.assertIsNone(result.evidence)

    def test_lossless_transcode_current_evidence_missing_v0_fails_closed(self):
        stale_evidence_id = self._persist_lossless_transcode_current_without_v0()

        result = ensure_current_evidence_for_action(
            self.db,
            request_id=42,
            mb_release_id="release-1",
            current_release=self._current_release(),
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

    def _persist_blank_path_current(self) -> int:
        """Legacy backfill shape: matching snapshot, empty source_path.

        The download_log 37206 (French Quarter) row: a 2026-05-16 library
        backfill wrote current evidence with ``source_path=''``, which no
        enrichment helper can ever complete (every persist guard compares
        against the recorded path), so the import decision stayed
        spectrally blind forever.
        """
        evidence = make_album_quality_evidence(
            mb_release_id="release-1",
            source_path="",
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

    def test_blank_source_path_current_evidence_is_never_loaded(self):
        self._persist_blank_path_current()

        result = ensure_current_evidence_for_action(
            self.db,
            request_id=42,
            mb_release_id="release-1",
            current_release=self._current_release(),
            album_info=AlbumInfo(
                album_id=1,
                track_count=1,
                min_bitrate_kbps=186,
                avg_bitrate_kbps=194,
                median_bitrate_kbps=194,
                is_cbr=False,
                album_path=self.root,
                format="MP3",
            ),
        )

        self.assertNotEqual(result.provenance.current_status, "loaded")
        self.assertTrue(result.available)
        self.assertEqual(result.provenance.current_status, "backfilled")
        self.assertIn(
            "source_path",
            result.provenance.fallback_reason or "",
        )
        assert result.evidence is not None
        self.assertEqual(result.evidence.source_path, self.root)

    def test_blank_source_path_rebuild_repairs_the_row_in_place(self):
        """Same files ⇒ same content address ⇒ the upsert must repair
        ``source_path`` on the linked row so enrichment can complete it."""
        stale_id = self._persist_blank_path_current()

        ensure_current_evidence_for_action(
            self.db,
            request_id=42,
            mb_release_id="release-1",
            current_release=self._current_release(),
            album_info=AlbumInfo(
                album_id=1,
                track_count=1,
                min_bitrate_kbps=186,
                avg_bitrate_kbps=194,
                median_bitrate_kbps=194,
                is_cbr=False,
                album_path=self.root,
                format="MP3",
            ),
        )

        linked_id = self.db.get_request_current_evidence_id(42)
        self.assertEqual(linked_id, stale_id)
        linked = self.db.load_album_quality_evidence_by_id(linked_id)
        assert linked is not None
        self.assertEqual(linked.source_path, self.root)

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
                current_release=self._current_release(),
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
            track_count=3,
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

    def _beets(self, *, present: bool = True) -> FakeBeetsDB:
        beets = FakeBeetsDB(library_root=self.root)
        beets.set_album_info(
            "release-1",
            self.album_info if present else None,
        )
        return beets

    def test_happy_path_returns_ensure_result_unchanged(self):
        expected = self._available_result()
        beets = self._beets()
        with patch("lib.beets_db.BeetsDB", return_value=beets), patch(
            "lib.import_evidence.ensure_current_evidence_for_action",
            return_value=expected,
        ) as ensure:
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
        self.assertEqual(kwargs["album_info"].album_path, self.root)
        self.assertEqual(kwargs["current_release"].album_path, self.root)
        self.assertEqual(kwargs["beets_library_root"], "/tmp/beets")
        self.assertEqual(len(beets.resolve_current_release_calls), 1)

    def test_beets_absent_returns_none(self):
        with patch(
            "lib.beets_db.BeetsDB",
            return_value=self._beets(present=False),
        ), patch(
            "lib.import_evidence.ensure_current_evidence_for_action"
        ) as ensure:
            result = load_current_evidence_for_action(
                self.db,
                request_id=42,
                mb_release_id="release-1",
            )

        self.assertIsNone(result)
        ensure.assert_not_called()

    def test_explicit_beets_library_path_is_forwarded(self):
        with patch(
            "lib.beets_db.BeetsDB",
            return_value=self._beets(present=False),
        ) as beets_cls:
            load_current_evidence_for_action(
                self.db,
                request_id=42,
                mb_release_id="release-1",
                beets_library_db_path="/tmp/world/beets-library.db",
                beets_library_root="/tmp/world/library",
            )

        beets_cls.assert_called_once_with(
            "/tmp/world/beets-library.db",
            library_root="/tmp/world/library",
        )

    def test_ensure_raises_returns_fail_closed_result(self):
        with patch("lib.beets_db.BeetsDB", return_value=self._beets()), patch(
            "lib.import_evidence.ensure_current_evidence_for_action",
            side_effect=RuntimeError("backfill failed"),
        ):
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
        with patch("lib.beets_db.BeetsDB", return_value=self._beets()), patch(
            "lib.import_evidence.ensure_current_evidence_for_action",
            return_value=fail_closed,
        ):
            result = load_current_evidence_for_action(
                self.db,
                request_id=42,
                mb_release_id="release-1",
            )

        self.assertIs(result, fail_closed)

    def test_default_quality_ranks_resolves_to_defaults(self):
        with patch("lib.beets_db.BeetsDB", return_value=self._beets()), patch(
            "lib.import_evidence.ensure_current_evidence_for_action",
            return_value=self._available_result(),
        ) as ensure:
            load_current_evidence_for_action(
                self.db,
                request_id=42,
                mb_release_id="release-1",
            )

        self.assertEqual(
            ensure.call_args.kwargs["quality_ranks"],
            QualityRankConfig.defaults(),
        )


if __name__ == "__main__":
    unittest.main()
