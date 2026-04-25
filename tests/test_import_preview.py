"""Tests for unified import preview service."""

import os
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from lib.config import CratediggerConfig
from lib.import_preview import (
    ImportPreviewValues,
    preview_import_from_path,
    preview_import_from_values,
)
from lib.preimport import LocalFileInspection, PreImportGateResult
from lib.quality import AudioQualityMeasurement, ImportResult, full_pipeline_decision
from tests.fakes import FakePipelineDB
from tests.helpers import make_request_row


class TestImportPreviewValues(unittest.TestCase):
    def test_values_preview_delegates_to_full_pipeline_shape(self):
        values = ImportPreviewValues(
            is_flac=False,
            min_bitrate=245,
            avg_bitrate=245,
            is_cbr=False,
            is_vbr=True,
        )

        preview = preview_import_from_values(values)

        direct = full_pipeline_decision(
            is_flac=False,
            min_bitrate=245,
            avg_bitrate=245,
            is_cbr=False,
            is_vbr=True,
        )
        self.assertEqual(preview.simulation, direct)
        self.assertEqual(preview.verdict, "would_import")
        self.assertIn("stage2_import:import", preview.stage_chain)

    def test_values_preview_classifies_spectral_reject_as_confident(self):
        preview = preview_import_from_values(
            ImportPreviewValues(
                is_flac=False,
                min_bitrate=320,
                is_cbr=True,
                spectral_grade="suspect",
                spectral_bitrate=96,
                existing_spectral_bitrate=128,
            )
        )

        self.assertEqual(preview.verdict, "confident_reject")
        self.assertTrue(preview.cleanup_eligible)
        self.assertEqual(preview.reason, "spectral_reject")


class TestImportPreviewPath(unittest.TestCase):
    def _db(self) -> FakePipelineDB:
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            mb_release_id="mbid-42",
            status="manual",
            min_bitrate=180,
            artist_name="Artist",
            album_title="Album",
        ))
        return db

    def _source_dir(self) -> str:
        source = tempfile.mkdtemp()
        with open(os.path.join(source, "01.mp3"), "wb") as handle:
            handle.write(b"not real audio but never inspected in this test")
        return source

    def test_real_path_preview_runs_harness_dry_run_without_db_writes(self):
        db = self._db()
        source = self._source_dir()
        before = sorted(os.listdir(source))
        try:
            with patch("lib.config.read_runtime_config",
                       return_value=CratediggerConfig(
                           beets_harness_path="/fake/harness/run_beets_harness.sh",
                           pipeline_db_enabled=True,
                       )), \
                 patch("lib.import_preview.inspect_local_files",
                       return_value=LocalFileInspection(
                           filetype="mp3",
                           min_bitrate_bps=245000,
                           is_vbr=True,
                       )), \
                 patch("lib.import_preview.run_preimport_gates",
                       return_value=PreImportGateResult()), \
                 patch("lib.import_preview.run_import_one",
                       return_value=SimpleNamespace(
                           import_result=ImportResult(
                               decision="import",
                               new_measurement=AudioQualityMeasurement(
                                   min_bitrate_kbps=245,
                                   avg_bitrate_kbps=245,
                                   median_bitrate_kbps=245,
                                   format="mp3 v0",
                               ),
                           )
                       )) as mock_run:
                preview = preview_import_from_path(
                    db,
                    request_id=42,
                    path=source,
                )

            self.assertEqual(preview.verdict, "would_import")
            self.assertEqual(sorted(os.listdir(source)), before)
            self.assertEqual(db.download_logs, [])
            self.assertEqual(db.denylist, [])
            self.assertTrue(mock_run.call_args.kwargs["dry_run"])
            self.assertIsNone(mock_run.call_args.kwargs["request_id"])
        finally:
            import shutil
            shutil.rmtree(source, ignore_errors=True)

    def test_preimport_reject_is_confident_without_denylist_side_effects(self):
        db = self._db()
        source = self._source_dir()
        try:
            with patch("lib.config.read_runtime_config",
                       return_value=CratediggerConfig(
                           beets_harness_path="/fake/harness/run_beets_harness.sh",
                           pipeline_db_enabled=True,
                       )), \
                 patch("lib.import_preview.inspect_local_files",
                       return_value=LocalFileInspection(
                           filetype="mp3",
                           min_bitrate_bps=128000,
                           is_vbr=False,
                       )), \
                 patch("lib.import_preview.run_preimport_gates",
                       return_value=PreImportGateResult(
                           valid=False,
                           scenario="spectral_reject",
                           detail="spectral 96kbps <= existing 128kbps",
                       )), \
                 patch("lib.import_preview.run_import_one") as mock_run:
                preview = preview_import_from_path(
                    db,
                    request_id=42,
                    path=source,
                )

            self.assertEqual(preview.verdict, "confident_reject")
            self.assertTrue(preview.cleanup_eligible)
            self.assertEqual(preview.decision, "spectral_reject")
            self.assertEqual(db.denylist, [])
            mock_run.assert_not_called()
        finally:
            import shutil
            shutil.rmtree(source, ignore_errors=True)

    def test_missing_path_is_uncertain_not_cleanup_eligible(self):
        preview = preview_import_from_path(
            self._db(),
            request_id=42,
            path="/tmp/definitely-missing-cratedigger-preview",
        )

        self.assertEqual(preview.verdict, "uncertain")
        self.assertEqual(preview.decision, "path_missing")
        self.assertFalse(preview.cleanup_eligible)


if __name__ == "__main__":
    unittest.main()
