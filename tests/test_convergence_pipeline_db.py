"""Real PostgreSQL convergence derivation and atomic stop round trip."""

from __future__ import annotations

import unittest

from lib.convergence_service import ConvergenceStopService
from lib.pipeline_db import DownloadLogOutcome
from lib.quality import (
    AlbumQualityEvidenceFile,
    AlbumQualityV0Metric,
    AudioQualityMeasurement,
)
from tests.helpers import make_album_quality_evidence
from tests.test_pipeline_db import make_db, requires_postgres


@requires_postgres
class TestConvergencePipelineDB(unittest.TestCase):
    def setUp(self) -> None:
        self.db = make_db()
        self.request_id = self.db.add_request(
            "Convergence Artist",
            "Convergence Album",
            "request",
            mb_release_id="convergence-release",
            status="wanted",
        )
        current = make_album_quality_evidence(
            mb_release_id="convergence-release",
            source_path="/library/convergence",
            measurement=AudioQualityMeasurement(format="MP3"),
            v0_metric=AlbumQualityV0Metric(
                subject="source",
                provenance="measured",
                min_bitrate_kbps=220,
                avg_bitrate_kbps=230,
            ),
        )
        self.db.upsert_album_quality_evidence(current)
        stored = self.db.find_album_quality_evidence(
            mb_release_id=current.mb_release_id,
            snapshot_fingerprint=current.snapshot_fingerprint,
        )
        assert stored is not None and stored.id is not None
        self.current_evidence_id = stored.id
        self.assertTrue(self.db.set_request_current_evidence(
            self.request_id, stored.id,
        ))

    def tearDown(self) -> None:
        self.db.close()

    def _add_observation(
        self,
        peer: str,
        *,
        cliff_hz: int | None = 15_000,
        outcome: DownloadLogOutcome = "rejected",
        beets_distance: float = 0.05,
        beets_scenario: str = "strong_match",
        measurement_version: int = 2,
    ) -> int:
        log_id = self.db.log_download(
            self.request_id,
            soulseek_username=peer,
            filetype="flac",
            beets_distance=beets_distance,
            beets_scenario=beets_scenario,
            outcome=outcome,
        )
        evidence = make_album_quality_evidence(
            mb_release_id="convergence-release",
            source_path=f"/candidate/{log_id}",
            files=[AlbumQualityEvidenceFile(
                relative_path="01.flac",
                size_bytes=10_000 + log_id,
                mtime_ns=1_700_000_000_000_000_000 + log_id,
                extension="flac",
                container="flac",
                codec="flac",
            )],
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=800,
                avg_bitrate_kbps=900,
                median_bitrate_kbps=850,
                format="FLAC",
                spectral_grade="genuine",
                spectral_subject="source",
                spectral_provenance="measured",
                cliff_hz=cliff_hz,
                codec_family="lossless",
                ultrasonic_deficit_db=8.5,
                spectral_measurement_version=measurement_version,
            ),
            codec="flac",
            container="flac",
            storage_format="FLAC",
            preserve_spectral_measurement_version=True,
        )
        self.db.upsert_album_quality_evidence(evidence)
        stored = self.db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert stored is not None and stored.id is not None
        self.db.set_download_log_candidate_evidence(log_id, stored.id)
        return log_id

    def test_threshold_stale_rejection_stop_and_resume(self) -> None:
        for index in range(4):
            self._add_observation(f"peer-{index}")
        self.assertEqual(self.db.get_convergence_signals([self.request_id]), {})

        captured_latest_log_id = self._add_observation("peer-4")
        signal = self.db.get_convergence_signals([self.request_id])[self.request_id]
        self.assertEqual(signal.distinct_peer_count, 5)
        self.assertEqual(signal.cliff_hz, 15_000)
        self.assertEqual(
            signal.latest_qualifying_log_id, captured_latest_log_id,
        )

        # Newer world errors, non-exact candidates, high-distance matches,
        # and legacy measurements are invisible to the eligible sequence.
        self._add_observation("failed-world", outcome="failed")
        self._add_observation(
            "non-exact", beets_scenario="high_distance",
        )
        self._add_observation("too-distant", beets_distance=0.16)
        self._add_observation("legacy-measurement", measurement_version=1)
        unchanged = self.db.get_convergence_signals(
            [self.request_id],
        )[self.request_id]
        self.assertEqual(unchanged.observation_count, 5)
        self.assertEqual(
            unchanged.latest_qualifying_log_id, captured_latest_log_id,
        )

        # Client captured the five-peer signal, but a sixth eligible world
        # landed before the action. The action must lock and rederive, then
        # reject that now-stale exact signal identity.
        latest_log_id = self._add_observation("peer-5")

        stale = ConvergenceStopService(self.db).stop(
            self.request_id,
            latest_qualifying_log_id=captured_latest_log_id,
            cliff_hz=15_000,
        )
        self.assertEqual(stale.outcome, "stale")
        request = self.db.get_request(self.request_id)
        assert request is not None
        self.assertEqual(request["status"], "wanted")

        before_count = int(self.db._execute(
            "SELECT COUNT(*) AS n FROM album_quality_evidence",
        ).fetchone()["n"])
        stopped = ConvergenceStopService(self.db).stop(
            self.request_id,
            latest_qualifying_log_id=latest_log_id,
            cliff_hz=15_000,
        )
        self.assertEqual(stopped.outcome, "stopped")
        request = self.db.get_request(self.request_id)
        assert request is not None
        self.assertEqual(request["status"], "unsearchable")
        self.assertEqual(request["current_evidence_id"], self.current_evidence_id)
        after_count = int(self.db._execute(
            "SELECT COUNT(*) AS n FROM album_quality_evidence",
        ).fetchone()["n"])
        self.assertEqual(after_count, before_count)

        self.assertTrue(self.db.update_status(
            self.request_id, "wanted", expected_status="unsearchable",
        ))
        resumed = self.db.get_request(self.request_id)
        assert resumed is not None
        self.assertEqual(resumed["status"], "wanted")

        self._add_observation("upward-break", cliff_hz=None)
        self.assertEqual(self.db.get_convergence_signals([self.request_id]), {})
        not_converged = ConvergenceStopService(self.db).stop(
            self.request_id,
            latest_qualifying_log_id=latest_log_id,
            cliff_hz=15_000,
        )
        self.assertEqual(not_converged.outcome, "not_converged")
        final = self.db.get_request(self.request_id)
        assert final is not None
        self.assertEqual(final["status"], "wanted")


if __name__ == "__main__":
    unittest.main()
