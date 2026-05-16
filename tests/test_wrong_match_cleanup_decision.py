"""Tests for :mod:`lib.wrong_match_cleanup_decision` FK-chain evidence lookup.

U4 of plan 2026-05-16-002: ``decide_wrong_match_cleanup`` must resolve
candidate evidence by walking the FK chain instead of recomputing every
time. The cold-path re-measurement (``preview_builder``) fires only for
genuinely evidence-less legacy rows or for stale snapshots (files
changed). This module owns the per-scenario coverage of that contract.
"""

from __future__ import annotations

import os
import tempfile
import types
import unittest
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import MagicMock, patch

from lib.import_preview import ImportPreviewResult
from lib.quality import (
    AlbumQualityEvidence,
    AlbumQualityEvidenceFile,
    AudioQualityMeasurement,
    QualityRankConfig,
)
from lib.quality_evidence import snapshot_fingerprint
from lib.wrong_match_cleanup_decision import decide_wrong_match_cleanup
from tests.fakes import FakePipelineDB
from tests.helpers import make_request_row


def _fake_cfg() -> types.SimpleNamespace:
    """Minimal cfg surface that ``decide_wrong_match_cleanup`` reads."""
    return types.SimpleNamespace(
        quality_ranks=QualityRankConfig.defaults(),
        verified_lossless_target="",
        beets_directory="",
    )


def _populate_source_with_track(source_dir: str, name: str = "01.mp3") -> str:
    """Write a single audio file under ``source_dir`` and return its path."""
    path = os.path.join(source_dir, name)
    with open(path, "wb") as handle:
        handle.write(b"audio")
    return path


def _evidence_files_for(
    source_dir: str,
    relative_paths: tuple[str, ...] = ("01.mp3",),
) -> list[AlbumQualityEvidenceFile]:
    """Build evidence-file rows matching the on-disk files under ``source_dir``.

    Uses the live ``stat`` size so ``audio_snapshot_matches`` returns True.
    """
    files: list[AlbumQualityEvidenceFile] = []
    for rel in relative_paths:
        full = os.path.join(source_dir, rel)
        stat = os.stat(full)
        ext = os.path.splitext(rel)[1].lstrip(".").lower()
        files.append(
            AlbumQualityEvidenceFile(
                relative_path=rel,
                size_bytes=int(stat.st_size),
                mtime_ns=int(stat.st_mtime_ns),
                extension=ext,
                container=ext,
                codec=ext,
            )
        )
    return files


def _build_evidence(
    *,
    mb_release_id: str,
    source_dir: str,
    relative_paths: tuple[str, ...] = ("01.mp3",),
    measured_at: datetime | None = None,
) -> AlbumQualityEvidence:
    """Build a content-addressed evidence row matching files in ``source_dir``."""
    files = _evidence_files_for(source_dir, relative_paths)
    return AlbumQualityEvidence(
        mb_release_id=mb_release_id,
        snapshot_fingerprint=snapshot_fingerprint(files),
        source_path=source_dir,
        measurement=AudioQualityMeasurement(
            min_bitrate_kbps=245,
            avg_bitrate_kbps=256,
            median_bitrate_kbps=252,
            format="mp3 v0",
            spectral_grade="genuine",
            spectral_bitrate_kbps=None,
        ),
        measured_at=measured_at or datetime(2026, 5, 1, tzinfo=timezone.utc),
        files=files,
        codec="mp3",
        container="mp3",
        storage_format="mp3 v0",
        audio_file_count=len(files),
        filetype_band="mp3",
        folder_layout="flat",
    )


class _Base(unittest.TestCase):
    """Common fixtures: seeded request + a wrong-match download_log row."""

    def setUp(self) -> None:
        self.source = tempfile.mkdtemp()
        _populate_source_with_track(self.source)
        self.db = FakePipelineDB()
        self.db.seed_request(make_request_row(
            id=1,
            status="manual",
            mb_release_id="mbid-1",
        ))
        self.db.log_download(
            1,
            outcome="rejected",
            validation_result={
                "scenario": "wrong_match",
                "failed_path": self.source,
            },
        )
        self.log_id = self.db.download_logs[-1].id

    def tearDown(self) -> None:
        import shutil
        shutil.rmtree(self.source, ignore_errors=True)

    def _patch_beets_album_info_none(self) -> Any:
        """Patch BeetsDB so the album-not-in-beets branch runs.

        ``decide_wrong_match_cleanup`` looks up the current album via beets
        and tolerates ``None`` (album not yet imported). This is the
        natural fixture for wrong-match cleanup: the candidate is on disk,
        the album isn't in beets yet.
        """
        ctx = MagicMock()
        ctx.__enter__ = MagicMock(return_value=ctx)
        ctx.__exit__ = MagicMock(return_value=False)
        ctx.get_album_info.return_value = None
        return patch("lib.beets_db.BeetsDB", return_value=ctx)


class TestEvidenceFromDownloadLogFK(_Base):
    """Scenario 1: ``download_log.candidate_evidence_id`` is set."""

    def test_returns_evidence_from_download_log_fk_no_remeasure(self) -> None:
        evidence = _build_evidence(
            mb_release_id="mbid-1",
            source_dir=self.source,
        )
        self.db.upsert_album_quality_evidence(evidence)
        stored = self.db.find_album_quality_evidence(
            mb_release_id="mbid-1",
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert stored is not None and stored.id is not None
        self.db.set_download_log_candidate_evidence(self.log_id, stored.id)

        preview_builder = MagicMock(name="preview_builder")

        with self._patch_beets_album_info_none(), \
                patch("lib.preimport.measure_preimport_state") as mp:
            decision = decide_wrong_match_cleanup(
                self.db,
                self.log_id,
                preview_builder=preview_builder,
                cfg=_fake_cfg(),
            )

        # The FK chain resolved evidence without re-measuring.
        preview_builder.assert_not_called()
        mp.assert_not_called()
        self.assertFalse(decision.uncertain)
        self.assertEqual(decision.request_id, 1)


class TestEvidenceFromSiblingImportJobFK(_Base):
    """Scenario 2: download_log FK is NULL but a sibling import_job has it."""

    def test_cross_walk_picks_up_sibling_import_job_evidence(self) -> None:
        evidence = _build_evidence(
            mb_release_id="mbid-1",
            source_dir=self.source,
        )
        self.db.upsert_album_quality_evidence(evidence)
        stored = self.db.find_album_quality_evidence(
            mb_release_id="mbid-1",
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert stored is not None and stored.id is not None

        # Seed a sibling import_job for the same request, with the FK set.
        job = self.db.enqueue_import_job(
            "automation_import",
            request_id=1,
            payload={},
        )
        self.db.set_import_job_candidate_evidence(job.id, stored.id)

        # download_log FK is NULL.
        self.assertIsNone(
            self.db.get_download_log_candidate_evidence_id(self.log_id),
        )

        preview_builder = MagicMock(name="preview_builder")

        with self._patch_beets_album_info_none(), \
                patch("lib.preimport.measure_preimport_state") as mp:
            decision = decide_wrong_match_cleanup(
                self.db,
                self.log_id,
                preview_builder=preview_builder,
                cfg=_fake_cfg(),
            )

        preview_builder.assert_not_called()
        mp.assert_not_called()
        self.assertFalse(decision.uncertain)


class TestMostRecentImportJobWinsCrossWalk(_Base):
    """Scenario 3: most recent (created_at DESC) import_job FK wins."""

    def test_most_recent_import_job_evidence_wins(self) -> None:
        # Two evidence rows, distinguishable by measurement.format.
        old_files = _evidence_files_for(self.source)
        old_evidence = AlbumQualityEvidence(
            mb_release_id="mbid-1",
            snapshot_fingerprint=snapshot_fingerprint(old_files),
            source_path=self.source,
            measurement=AudioQualityMeasurement(format="OLD"),
            measured_at=datetime(2026, 4, 1, tzinfo=timezone.utc),
            files=old_files,
            audio_file_count=len(old_files),
            filetype_band="mp3",
            folder_layout="flat",
        )
        # Both evidence rows must address the same fileset (same source),
        # so they share the same (mb_release_id, snapshot_fingerprint) key.
        # Use a distinct mb_release_id for the older row to keep both rows
        # in the FakeDB. This mirrors the production case where different
        # release IDs (or different snapshots) co-exist.
        old_evidence_distinct = AlbumQualityEvidence(
            mb_release_id="mbid-1-stale",
            snapshot_fingerprint=snapshot_fingerprint(old_files),
            source_path=self.source,
            measurement=AudioQualityMeasurement(format="OLD"),
            measured_at=datetime(2026, 4, 1, tzinfo=timezone.utc),
            files=old_files,
            audio_file_count=len(old_files),
            filetype_band="mp3",
            folder_layout="flat",
        )
        new_evidence = _build_evidence(
            mb_release_id="mbid-1",
            source_dir=self.source,
        )
        self.db.upsert_album_quality_evidence(old_evidence_distinct)
        self.db.upsert_album_quality_evidence(new_evidence)
        old_stored = self.db.find_album_quality_evidence(
            mb_release_id="mbid-1-stale",
            snapshot_fingerprint=old_evidence_distinct.snapshot_fingerprint,
        )
        new_stored = self.db.find_album_quality_evidence(
            mb_release_id="mbid-1",
            snapshot_fingerprint=new_evidence.snapshot_fingerprint,
        )
        assert old_stored is not None and old_stored.id is not None
        assert new_stored is not None and new_stored.id is not None

        # Older job → older evidence.
        old_job = self.db.enqueue_import_job(
            "automation_import",
            request_id=1,
            dedupe_key="auto:1:old",
            payload={},
        )
        self.db.set_import_job_candidate_evidence(old_job.id, old_stored.id)
        # Backdate the old job so the most-recent ordering is unambiguous.
        for row in self.db._import_jobs:
            if row["id"] == old_job.id:
                row["created_at"] = (
                    datetime.now(timezone.utc) - timedelta(days=1)
                )

        # Newer job → newer evidence.
        new_job = self.db.enqueue_import_job(
            "automation_import",
            request_id=1,
            dedupe_key="auto:1:new",
            payload={},
        )
        self.db.set_import_job_candidate_evidence(new_job.id, new_stored.id)

        # download_log FK is NULL → must cross-walk; should pick new_stored.
        with self._patch_beets_album_info_none(), \
                patch("lib.preimport.measure_preimport_state") as mp:
            decision = decide_wrong_match_cleanup(
                self.db,
                self.log_id,
                preview_builder=MagicMock(name="should_not_run"),
                cfg=_fake_cfg(),
            )

        mp.assert_not_called()
        self.assertFalse(decision.uncertain)
        # The decision's import_result captures the candidate measurement;
        # confirm we got the *new* evidence (format="mp3 v0"), not the
        # old one (format="OLD").
        assert decision.import_result is not None
        new_meas = decision.import_result.new_measurement
        assert new_meas is not None
        self.assertEqual(new_meas.format, "mp3 v0")


class TestColdPathFallbackOnGenuinelyMissingEvidence(_Base):
    """Scenario 4: no FK chain at all → fall back + log WARN."""

    def test_cold_path_fires_with_warn_log_when_no_fk_chain(self) -> None:
        # No evidence rows seeded; no import_jobs; download_log FK NULL.
        called_with: dict[str, Any] = {}

        def fake_preview_builder(db: Any, dlid: int) -> ImportPreviewResult:
            # Seed evidence as a side effect, mirroring what the real
            # preview path does via persist_candidate_evidence=True.
            evidence = _build_evidence(
                mb_release_id="mbid-1",
                source_dir=self.source,
            )
            db.upsert_album_quality_evidence(evidence)
            stored = db.find_album_quality_evidence(
                mb_release_id="mbid-1",
                snapshot_fingerprint=evidence.snapshot_fingerprint,
            )
            assert stored is not None and stored.id is not None
            db.set_download_log_candidate_evidence(dlid, stored.id)
            called_with["dlid"] = dlid
            return ImportPreviewResult(
                mode="download_log",
                verdict="would_import",
                would_import=True,
                decision="import",
                reason="import",
                source_path=self.source,
            )

        with self._patch_beets_album_info_none(), \
                self.assertLogs(
                    "cratedigger", level="WARNING") as captured:
            decision = decide_wrong_match_cleanup(
                self.db,
                self.log_id,
                preview_builder=fake_preview_builder,
                cfg=_fake_cfg(),
            )

        # Cold path fired exactly once and the WARN log identifies the row.
        self.assertEqual(called_with.get("dlid"), self.log_id)
        warn_lines = [
            line for line in captured.output
            if line.startswith("WARNING:") and "cold_path_fallback" in line
        ]
        self.assertEqual(
            len(warn_lines),
            1,
            f"expected exactly one cold-path WARN; got {captured.output}",
        )
        self.assertIn(f"download_log_id={self.log_id}", warn_lines[0])
        self.assertIn("candidate_status=missing", warn_lines[0])
        # The decision still resolved post-fallback.
        self.assertFalse(decision.uncertain)


class TestStaleSnapshotReMeasures(_Base):
    """Scenario 5: snapshot fingerprint mismatch → re-measure (existing)."""

    def test_stale_snapshot_falls_back_to_preview_builder(self) -> None:
        # Seed evidence whose snapshot_fingerprint matches the *current*
        # state of the source dir; then mutate the source so the snapshot
        # no longer matches. ``audio_snapshot_matches`` returns False, the
        # FK chain marks the candidate "stale", and the cold path runs to
        # re-measure (this is the only mantra-aligned re-measurement).
        evidence = _build_evidence(
            mb_release_id="mbid-1",
            source_dir=self.source,
        )
        self.db.upsert_album_quality_evidence(evidence)
        stored = self.db.find_album_quality_evidence(
            mb_release_id="mbid-1",
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert stored is not None and stored.id is not None
        self.db.set_download_log_candidate_evidence(self.log_id, stored.id)

        # Mutate the source: add a second audio file. Snapshot no longer
        # matches the stored evidence's files list.
        with open(os.path.join(self.source, "02.mp3"), "wb") as fp:
            fp.write(b"more audio")

        called: dict[str, Any] = {}

        def fake_preview_builder(db: Any, dlid: int) -> ImportPreviewResult:
            # Persist fresh evidence reflecting the new on-disk state.
            new_evidence = _build_evidence(
                mb_release_id="mbid-1",
                source_dir=self.source,
                relative_paths=("01.mp3", "02.mp3"),
            )
            db.upsert_album_quality_evidence(new_evidence)
            stored_new = db.find_album_quality_evidence(
                mb_release_id="mbid-1",
                snapshot_fingerprint=new_evidence.snapshot_fingerprint,
            )
            assert stored_new is not None and stored_new.id is not None
            db.set_download_log_candidate_evidence(dlid, stored_new.id)
            called["fired"] = True
            return ImportPreviewResult(
                mode="download_log",
                verdict="would_import",
                would_import=True,
                decision="import",
                reason="import",
                source_path=self.source,
            )

        with self._patch_beets_album_info_none():
            decision = decide_wrong_match_cleanup(
                self.db,
                self.log_id,
                preview_builder=fake_preview_builder,
                cfg=_fake_cfg(),
            )

        self.assertTrue(called.get("fired"))
        self.assertFalse(decision.uncertain)


if __name__ == "__main__":
    unittest.main()
