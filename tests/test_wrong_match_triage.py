"""Tests for preview-driven Wrong Matches triage."""

import os
import shutil
import tempfile
import unittest
from unittest.mock import patch

from lib.import_preview import ImportPreviewResult
from lib.import_queue import IMPORT_JOB_FORCE, force_import_dedupe_key, force_import_payload
from lib.quality import (
    AudioQualityMeasurement,
    ImportResult,
    V0_PROBE_LOSSLESS_SOURCE,
    V0ProbeEvidence,
)
from lib.wrong_match_triage import (
    backfill_wrong_match_previews,
    triage_wrong_match,
    triage_wrong_matches,
)
from tests.fakes import FakePipelineDB
from tests.helpers import make_request_row


class TestWrongMatchTriage(unittest.TestCase):
    def _db_with_wrong_match(self, source: str) -> tuple[FakePipelineDB, int]:
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=1,
            status="manual",
            mb_release_id="mbid-1",
        ))
        db.log_download(
            1,
            outcome="rejected",
            validation_result={
                "scenario": "wrong_match",
                "failed_path": source,
            },
        )
        return db, db.download_logs[-1].id

    def test_confident_cleanup_eligible_reject_deletes_and_clears(self):
        source = tempfile.mkdtemp()
        try:
            with open(os.path.join(source, "01.mp3"), "wb") as handle:
                handle.write(b"audio")
            db, log_id = self._db_with_wrong_match(source)

            with patch(
                "lib.wrong_match_triage.preview_import_from_download_log",
                return_value=ImportPreviewResult(
                    mode="download_log",
                    verdict="confident_reject",
                    confident_reject=True,
                    cleanup_eligible=True,
                    decision="downgrade",
                    reason="downgrade",
                    source_path=source,
                ),
            ):
                result = triage_wrong_match(db, log_id)

            self.assertEqual(result.action, "deleted_reject")
            self.assertTrue(result.success)
            self.assertFalse(os.path.exists(source))
            self.assertEqual(db.get_wrong_matches(), [])
            entry = db.get_download_log_entry(log_id)
            assert entry is not None
            vr = entry["validation_result"]
            assert isinstance(vr, dict)
            audit = vr["wrong_match_triage"]
            assert isinstance(audit, dict)
            self.assertEqual(audit["action"], "deleted_reject")
            self.assertEqual(audit["preview_decision"], "downgrade")
            self.assertEqual(audit["reason"], "downgrade")
            self.assertNotIn("failed_path", vr)
        finally:
            shutil.rmtree(source, ignore_errors=True)

    def test_would_import_leaves_candidate_visible(self):
        source = tempfile.mkdtemp()
        try:
            db, log_id = self._db_with_wrong_match(source)

            with patch(
                "lib.wrong_match_triage.preview_import_from_download_log",
                return_value=ImportPreviewResult(
                    mode="download_log",
                    verdict="would_import",
                    would_import=True,
                    decision="import",
                    reason="import",
                    source_path=source,
                ),
            ):
                result = triage_wrong_match(db, log_id)

            self.assertEqual(result.action, "kept_would_import")
            self.assertTrue(os.path.isdir(source))
            self.assertEqual(len(db.get_wrong_matches()), 1)
            entry = db.get_download_log_entry(log_id)
            assert entry is not None
            vr = entry["validation_result"]
            assert isinstance(vr, dict)
            audit = vr["wrong_match_triage"]
            assert isinstance(audit, dict)
            self.assertEqual(audit["action"], "kept_would_import")
            self.assertEqual(audit["preview_verdict"], "would_import")
            self.assertEqual(audit["reason"], "import")
        finally:
            shutil.rmtree(source, ignore_errors=True)

    def test_import_stage_requeue_preview_leaves_candidate_visible(self):
        source = tempfile.mkdtemp()
        try:
            with open(os.path.join(source, "01.mp3"), "wb") as handle:
                handle.write(b"audio")
            db, log_id = self._db_with_wrong_match(source)

            with patch(
                "lib.wrong_match_triage.preview_import_from_download_log",
                return_value=ImportPreviewResult(
                    mode="download_log",
                    verdict="confident_reject",
                    confident_reject=True,
                    cleanup_eligible=True,
                    decision="import",
                    reason="requeue_upgrade",
                    source_path=source,
                    stage_chain=[
                        "stage2_import:import",
                        "stage3_quality_gate:requeue_upgrade",
                    ],
                ),
            ):
                result = triage_wrong_match(db, log_id)

            self.assertEqual(result.action, "kept_would_import")
            self.assertTrue(result.success)
            self.assertTrue(os.path.isdir(source))
            self.assertEqual(len(db.get_wrong_matches()), 1)
            entry = db.get_download_log_entry(log_id)
            assert entry is not None
            vr = entry["validation_result"]
            assert isinstance(vr, dict)
            audit = vr["wrong_match_triage"]
            assert isinstance(audit, dict)
            self.assertEqual(audit["action"], "kept_would_import")
            self.assertEqual(audit["preview_verdict"], "confident_reject")
            self.assertEqual(audit["preview_decision"], "import")
            self.assertEqual(audit["reason"], "requeue_upgrade")
            self.assertIn("failed_path", vr)
        finally:
            shutil.rmtree(source, ignore_errors=True)

    def test_missing_path_clears_stale_pointer_without_successful_reject(self):
        source = tempfile.mkdtemp()
        shutil.rmtree(source)
        db, log_id = self._db_with_wrong_match(source)

        with patch(
            "lib.wrong_match_triage.preview_import_from_download_log",
            return_value=ImportPreviewResult(
                mode="download_log",
                verdict="uncertain",
                uncertain=True,
                decision="path_missing",
                reason="path_missing",
                source_path=source,
            ),
        ):
            result = triage_wrong_match(db, log_id)

        self.assertEqual(result.action, "stale_path_cleared")
        self.assertTrue(result.success)
        self.assertEqual(db.get_wrong_matches(), [])
        entry = db.get_download_log_entry(log_id)
        assert entry is not None
        vr = entry["validation_result"]
        assert isinstance(vr, dict)
        audit = vr["wrong_match_triage"]
        assert isinstance(audit, dict)
        self.assertEqual(audit["action"], "stale_path_cleared")
        self.assertEqual(audit["reason"], "path_missing")
        self.assertNotIn("failed_path", vr)

    def test_backfill_audit_only_records_resolvable_preview(self):
        source = tempfile.mkdtemp()
        try:
            with open(os.path.join(source, "01.mp3"), "wb") as handle:
                handle.write(b"audio")
            db, log_id = self._db_with_wrong_match(source)

            with patch(
                "lib.wrong_match_triage.preview_import_from_download_log",
                return_value=ImportPreviewResult(
                    mode="download_log",
                    verdict="would_import",
                    would_import=True,
                    decision="import",
                    reason="import",
                    source_path=source,
                ),
            ) as preview:
                summary = backfill_wrong_match_previews(db)

            preview.assert_called_once_with(db, log_id)
            self.assertEqual(summary["previewed"], 1)
            self.assertEqual(summary["would_import"], 1)
            self.assertEqual(len(db.get_wrong_matches()), 1)
            entry = db.get_download_log_entry(log_id)
            assert entry is not None
            vr = entry["validation_result"]
            assert isinstance(vr, dict)
            audit = vr["wrong_match_triage"]
            self.assertEqual(audit["action"], "preview_backfilled")
            self.assertEqual(audit["preview_verdict"], "would_import")
        finally:
            shutil.rmtree(source, ignore_errors=True)

    def test_backfill_dry_run_counts_cleanup_candidates_without_mutating(self):
        source = tempfile.mkdtemp()
        try:
            with open(os.path.join(source, "01.mp3"), "wb") as handle:
                handle.write(b"audio")
            db, log_id = self._db_with_wrong_match(source)

            with patch(
                "lib.wrong_match_triage.preview_import_from_download_log",
                return_value=ImportPreviewResult(
                    mode="download_log",
                    verdict="confident_reject",
                    confident_reject=True,
                    cleanup_eligible=True,
                    decision="requeue_upgrade",
                    reason="requeue_upgrade",
                    source_path=source,
                ),
            ) as preview:
                summary = backfill_wrong_match_previews(
                    db,
                    cleanup=True,
                    dry_run=True,
                )

            preview.assert_called_once_with(db, log_id)
            self.assertEqual(summary["previewed"], 1)
            self.assertEqual(summary["confident_reject"], 1)
            self.assertEqual(summary["cleanup_candidates"], 1)
            self.assertEqual(summary["cleaned"], 0)
            self.assertTrue(os.path.isdir(source))
            self.assertEqual(len(db.get_wrong_matches()), 1)
            entry = db.get_download_log_entry(log_id)
            assert entry is not None
            vr = entry["validation_result"]
            assert isinstance(vr, dict)
            self.assertNotIn("wrong_match_triage", vr)
            self.assertIn("failed_path", vr)
        finally:
            shutil.rmtree(source, ignore_errors=True)

    def test_backfill_non_positive_limit_does_not_process_rows(self):
        source = tempfile.mkdtemp()
        try:
            db, _log_id = self._db_with_wrong_match(source)

            with patch("lib.wrong_match_triage.preview_import_from_download_log") as preview:
                summary = backfill_wrong_match_previews(
                    db,
                    cleanup=True,
                    limit=0,
                )

            preview.assert_not_called()
            self.assertEqual(summary["previewed"], 0)
            self.assertTrue(os.path.isdir(source))
            self.assertEqual(len(db.get_wrong_matches()), 1)
        finally:
            shutil.rmtree(source, ignore_errors=True)

    def test_triage_non_positive_limit_does_not_process_rows(self):
        source = tempfile.mkdtemp()
        try:
            db, _log_id = self._db_with_wrong_match(source)

            with patch("lib.wrong_match_triage.preview_import_from_download_log") as preview:
                results = triage_wrong_matches(db, limit=0)

            preview.assert_not_called()
            self.assertEqual(results, [])
            self.assertTrue(os.path.isdir(source))
            self.assertEqual(len(db.get_wrong_matches()), 1)
        finally:
            shutil.rmtree(source, ignore_errors=True)

    def test_backfill_skips_missing_files_without_clearing(self):
        source = tempfile.mkdtemp()
        shutil.rmtree(source)
        db, _log_id = self._db_with_wrong_match(source)

        with patch("lib.wrong_match_triage.preview_import_from_download_log") as preview:
            summary = backfill_wrong_match_previews(db)

        preview.assert_not_called()
        self.assertEqual(summary["previewed"], 0)
        self.assertEqual(summary["skipped_missing_files"], 1)
        self.assertEqual(len(db.get_wrong_matches()), 1)

    def test_backfill_skips_active_force_import_job(self):
        source = tempfile.mkdtemp()
        try:
            db, log_id = self._db_with_wrong_match(source)
            db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=1,
                dedupe_key=force_import_dedupe_key(log_id),
                payload=force_import_payload(
                    download_log_id=log_id,
                    failed_path=source,
                ),
            )

            with patch("lib.wrong_match_triage.preview_import_from_download_log") as preview:
                summary = backfill_wrong_match_previews(db)

            preview.assert_not_called()
            self.assertEqual(summary["previewed"], 0)
            self.assertEqual(summary["skipped_active_jobs"], 1)
        finally:
            shutil.rmtree(source, ignore_errors=True)


class TestTriageMeasurementPersistence(unittest.TestCase):
    """Triage runs preview-with-spectral; the resulting measurement must
    land on the same download_log row that get_wrong_matches reads, so
    the per-candidate evidence cells from PR #181 actually populate.
    """

    def _seed(self, source: str) -> tuple[FakePipelineDB, int]:
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=1, status="manual", mb_release_id="mbid-1"))
        db.log_download(
            1,
            outcome="rejected",
            validation_result={
                "scenario": "wrong_match",
                "failed_path": source,
            },
        )
        return db, db.download_logs[-1].id

    def _measured_preview(
        self,
        source: str,
        *,
        spectral_grade: str | None = "genuine",
        spectral_bitrate_kbps: int | None = 950,
        v0_probe_avg_kbps: int | None = 265,
    ) -> ImportPreviewResult:
        new_measurement = (
            AudioQualityMeasurement(
                spectral_grade=spectral_grade,
                spectral_bitrate_kbps=spectral_bitrate_kbps,
            )
            if (spectral_grade is not None or spectral_bitrate_kbps is not None)
            else None
        )
        v0_probe = (
            V0ProbeEvidence(
                kind=V0_PROBE_LOSSLESS_SOURCE,
                avg_bitrate_kbps=v0_probe_avg_kbps,
            )
            if v0_probe_avg_kbps is not None
            else None
        )
        return ImportPreviewResult(
            mode="download_log",
            verdict="would_import",
            would_import=True,
            decision="import",
            reason="import",
            source_path=source,
            import_result=ImportResult(
                decision="import",
                new_measurement=new_measurement,
                v0_probe=v0_probe,
            ),
        )

    def test_would_import_persists_full_measurement(self):
        """Covers AE1: measurement reaches the row when preview produced one."""
        source = tempfile.mkdtemp()
        try:
            db, log_id = self._seed(source)
            with patch(
                "lib.wrong_match_triage.preview_import_from_download_log",
                return_value=self._measured_preview(source),
            ):
                triage_wrong_match(db, log_id)

            rows = db.get_wrong_matches()
            self.assertEqual(len(rows), 1)
            row = rows[0]
            self.assertEqual(row["spectral_grade"], "genuine")
            self.assertEqual(row["spectral_bitrate"], 950)
            self.assertEqual(row["v0_probe_kind"], "lossless_source_v0")
            self.assertEqual(row["v0_probe_avg_bitrate"], 265)
            # JSONB audit still recorded — sibling write, not a replacement.
            entry = db.get_download_log_entry(log_id)
            assert entry is not None
            vr = entry["validation_result"]
            assert isinstance(vr, dict)
            self.assertIn("wrong_match_triage", vr)
        finally:
            shutil.rmtree(source, ignore_errors=True)

    def test_early_reject_with_no_import_result_skips_measurement(self):
        """Covers AE3 / R5: nested_layout-style rejects have no measurement."""
        source = tempfile.mkdtemp()
        try:
            with open(os.path.join(source, "01.mp3"), "wb") as handle:
                handle.write(b"audio")
            db, log_id = self._seed(source)
            with patch(
                "lib.wrong_match_triage.preview_import_from_download_log",
                return_value=ImportPreviewResult(
                    mode="download_log",
                    verdict="confident_reject",
                    confident_reject=True,
                    cleanup_eligible=True,
                    decision="nested_layout",
                    reason="nested_layout",
                    source_path=source,
                    import_result=None,
                ),
            ):
                triage_wrong_match(db, log_id)

            # Row was deleted by cleanup_eligible reject; measurement
            # never persisted (verifies the early-return short-circuit
            # didn't raise on a None import_result).
            self.assertEqual(db.get_wrong_matches(), [])
            entry = db.get_download_log_entry(log_id)
            assert entry is not None
            vr = entry["validation_result"]
            assert isinstance(vr, dict)
            audit = vr["wrong_match_triage"]
            assert isinstance(audit, dict)
            self.assertEqual(audit["action"], "deleted_reject")
        finally:
            shutil.rmtree(source, ignore_errors=True)

    def test_spectral_only_measurement_leaves_v0_columns_untouched(self):
        """Spectral present, V0 probe absent → only spectral columns set."""
        source = tempfile.mkdtemp()
        try:
            db, log_id = self._seed(source)
            with patch(
                "lib.wrong_match_triage.preview_import_from_download_log",
                return_value=self._measured_preview(
                    source,
                    spectral_grade="marginal",
                    spectral_bitrate_kbps=800,
                    v0_probe_avg_kbps=None,
                ),
            ):
                triage_wrong_match(db, log_id)

            row = db.get_wrong_matches()[0]
            self.assertEqual(row["spectral_grade"], "marginal")
            self.assertEqual(row["spectral_bitrate"], 800)
            self.assertIsNone(row["v0_probe_kind"])
            self.assertIsNone(row["v0_probe_avg_bitrate"])
        finally:
            shutil.rmtree(source, ignore_errors=True)

    def test_v0_only_measurement_leaves_spectral_columns_untouched(self):
        """V0 probe present, spectral absent → only V0 columns set."""
        source = tempfile.mkdtemp()
        try:
            db, log_id = self._seed(source)
            with patch(
                "lib.wrong_match_triage.preview_import_from_download_log",
                return_value=self._measured_preview(
                    source,
                    spectral_grade=None,
                    spectral_bitrate_kbps=None,
                    v0_probe_avg_kbps=240,
                ),
            ):
                triage_wrong_match(db, log_id)

            row = db.get_wrong_matches()[0]
            self.assertIsNone(row["spectral_grade"])
            self.assertIsNone(row["spectral_bitrate"])
            self.assertEqual(row["v0_probe_kind"], "lossless_source_v0")
            self.assertEqual(row["v0_probe_avg_bitrate"], 240)
        finally:
            shutil.rmtree(source, ignore_errors=True)

    def test_import_result_with_no_measurements_is_noop(self):
        """import_result populated but every measurement field is None."""
        source = tempfile.mkdtemp()
        try:
            db, log_id = self._seed(source)
            with patch(
                "lib.wrong_match_triage.preview_import_from_download_log",
                return_value=ImportPreviewResult(
                    mode="download_log",
                    verdict="would_import",
                    would_import=True,
                    decision="import",
                    reason="import",
                    source_path=source,
                    import_result=ImportResult(
                        decision="import",
                        new_measurement=None,
                        v0_probe=None,
                    ),
                ),
            ):
                triage_wrong_match(db, log_id)

            row = db.get_wrong_matches()[0]
            for col in ("spectral_grade", "spectral_bitrate",
                        "v0_probe_kind", "v0_probe_avg_bitrate"):
                self.assertIsNone(row[col],
                                  f"{col} should remain None when measurement is empty")
            # JSONB audit still recorded.
            entry = db.get_download_log_entry(log_id)
            assert entry is not None
            vr = entry["validation_result"]
            assert isinstance(vr, dict)
            self.assertIn("wrong_match_triage", vr)
        finally:
            shutil.rmtree(source, ignore_errors=True)

    def test_backfill_persists_measurement(self):
        """Covers AE2: backfill CLI path produces the same outcome."""
        source = tempfile.mkdtemp()
        try:
            with open(os.path.join(source, "01.mp3"), "wb") as handle:
                handle.write(b"audio")
            db, _log_id = self._seed(source)
            with patch(
                "lib.wrong_match_triage.preview_import_from_download_log",
                return_value=self._measured_preview(source),
            ):
                summary = backfill_wrong_match_previews(db)

            self.assertEqual(summary["previewed"], 1)
            row = db.get_wrong_matches()[0]
            self.assertEqual(row["spectral_grade"], "genuine")
            self.assertEqual(row["spectral_bitrate"], 950)
            self.assertEqual(row["v0_probe_kind"], "lossless_source_v0")
            self.assertEqual(row["v0_probe_avg_bitrate"], 265)
        finally:
            shutil.rmtree(source, ignore_errors=True)

    def test_jsonb_audit_survives_measurement_write_failure(self):
        """Order regression: JSONB audit lands first, so measurement
        write raising still leaves the audit recorded."""
        source = tempfile.mkdtemp()
        try:
            db, log_id = self._seed(source)
            with patch(
                "lib.wrong_match_triage.preview_import_from_download_log",
                return_value=self._measured_preview(source),
            ), patch.object(
                db,
                "update_download_log_measurement",
                side_effect=RuntimeError("boom"),
            ):
                with self.assertRaises(RuntimeError):
                    triage_wrong_match(db, log_id)

            entry = db.get_download_log_entry(log_id)
            assert entry is not None
            vr = entry["validation_result"]
            assert isinstance(vr, dict)
            self.assertIn("wrong_match_triage", vr)
        finally:
            shutil.rmtree(source, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
