"""Real PostgreSQL convergence derivation and atomic stop round trip."""

from __future__ import annotations

import queue
import threading
import time
import unittest
from datetime import UTC, datetime, timedelta

import psycopg2

from lib.convergence_service import ConvergenceStopService, StopConvergedSearchResult
from lib.pipeline_db import DownloadLogOutcome, PipelineDB
from lib.quality import (
    AlbumQualityEvidenceFile,
    AlbumQualityV0Metric,
    AudioQualityMeasurement,
)
from tests.evidence_helpers import make_album_quality_evidence
from tests.test_pipeline_db import TEST_DSN, make_db, requires_postgres


@requires_postgres
class TestConvergenceIndexesExistInTheRealSchema(unittest.TestCase):
    """#1186. Migration 071's three partial indexes keep convergence reads off
    a multi-million-row ``download_log``.

    Until now nothing checked they EXIST. ``test_convergence_query_shape``
    pins their names in the migration's frozen text, which proves what the
    file once said, not what the database has — a triage mutant commenting
    out the whole ``CREATE INDEX idx_download_log_convergence_candidates``
    statement left both that text pin and every behavioural convergence test
    green. A later migration dropping one would be equally invisible, and
    text-level pinning cannot see it at all: only the live catalog can.
    """

    EXPECTED_INDEXES = (
        "idx_download_log_candidate_evidence_attribution",
        "idx_import_jobs_candidate_evidence_attribution",
        "idx_download_log_convergence_candidates",
    )

    def test_every_convergence_index_is_present_and_partial(self) -> None:
        db = make_db()
        try:
            cur = db._execute(
                "SELECT indexname, indexdef FROM pg_indexes "
                "WHERE indexname = ANY(%s)",
                (list(self.EXPECTED_INDEXES),),
            )
            definitions = {
                str(row["indexname"]): str(row["indexdef"]) for row in cur.fetchall()
            }
        finally:
            db.close()

        self.assertEqual(
            sorted(definitions), sorted(self.EXPECTED_INDEXES),
            "a convergence index is missing from the live schema",
        )
        for name, definition in definitions.items():
            # Partial is the whole point: a full index over download_log would
            # be the cost these exist to avoid.
            self.assertIn("WHERE", definition, name)

    def test_the_candidate_index_keeps_its_selective_predicate(self) -> None:
        """The predicate is what excludes cross-walked, non-Soulseek,
        non-exact and high-distance rows. An index of the same name with a
        widened predicate would satisfy a name-only check while quietly
        reintroducing the scan."""
        db = make_db()
        try:
            row = db._execute(
                "SELECT indexdef FROM pg_indexes WHERE indexname = %s",
                ("idx_download_log_convergence_candidates",),
            ).fetchone()
        finally:
            db.close()

        self.assertIsNotNone(row)
        assert row is not None
        definition = str(row["indexdef"])
        # Written against the catalog's OWN rendering, not the migration's
        # text: PostgreSQL normalises the predicate (adding casts and
        # parentheses), so `beets_distance <= 0.15` in the source file comes
        # back as `beets_distance <= (0.15)::double precision`. Asserting the
        # source spelling here would be the same mistake this class exists to
        # correct — believing the file over the database.
        for clause in (
            "candidate_evidence_direct IS TRUE",
            "source = 'slskd'::text",
            "beets_scenario = 'strong_match'::text",
            "beets_distance <= (0.15)::double precision",
        ):
            self.assertIn(clause, definition)


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
        codec: str = "flac",
        direct_attribution: bool = True,
        contributor_usernames: tuple[str, ...] | None = None,
        observed_at: datetime | None = None,
    ) -> int:
        contributors = (
            (peer,)
            if contributor_usernames is None
            else contributor_usernames
        )
        log_id = self.db.log_download(
            self.request_id,
            soulseek_username=peer,
            contributor_usernames=contributors,
            filetype=codec,
            beets_distance=beets_distance,
            beets_scenario=beets_scenario,
            outcome=outcome,
        )
        evidence = make_album_quality_evidence(
            mb_release_id="convergence-release",
            source_path=f"/candidate/{log_id}",
            files=[AlbumQualityEvidenceFile(
                relative_path=f"01.{codec}",
                size_bytes=10_000 + log_id,
                mtime_ns=1_700_000_000_000_000_000 + log_id,
                extension=codec,
                container=codec,
                codec=codec,
            )],
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=800,
                avg_bitrate_kbps=900,
                median_bitrate_kbps=850,
                format=codec.upper(),
                spectral_grade="genuine",
                spectral_subject="source",
                spectral_provenance="measured",
                cliff_hz=cliff_hz,
                codec_family="lossless",
                ultrasonic_deficit_db=8.5,
                spectral_measurement_version=measurement_version,
            ),
            codec=codec,
            container=codec,
            storage_format=codec.upper(),
            preserve_spectral_measurement_version=True,
        )
        self.db.upsert_album_quality_evidence(evidence)
        stored = self.db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert stored is not None and stored.id is not None
        self.db.set_download_log_candidate_evidence(
            log_id, stored.id, direct_attribution=direct_attribution,
        )
        if observed_at is not None:
            self.db._execute(
                "UPDATE download_log SET created_at = %s WHERE id = %s",
                (observed_at, log_id),
            )
            self.db.conn.commit()
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
            signal_token=signal.signal_token,
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
            signal_token=unchanged.signal_token,
        )
        self.assertEqual(stopped.outcome, "stale")
        current_signal = self.db.get_convergence_signals(
            [self.request_id],
        )[self.request_id]
        self.assertEqual(current_signal.latest_qualifying_log_id, latest_log_id)
        stopped = ConvergenceStopService(self.db).stop(
            self.request_id,
            signal_token=current_signal.signal_token,
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
            signal_token=current_signal.signal_token,
        )
        self.assertEqual(not_converged.outcome, "not_converged")
        final = self.db.get_request(self.request_id)
        assert final is not None
        self.assertEqual(final["status"], "wanted")

    def test_crosswalk_and_ambiguous_legacy_peer_text_fail_closed(self) -> None:
        for index, peer_set in enumerate(
            ["alice", "bob", "carol", "alice, bob", "alice, carol"],
        ):
            self._add_observation(
                peer_set, direct_attribution=False,
                contributor_usernames=(),
                observed_at=datetime(2026, 8, 3, tzinfo=UTC)
                + timedelta(seconds=index),
            )
        self.assertEqual(self.db.get_convergence_signals([self.request_id]), {})

        punctuation = (
            "comma,name", "semi;colon", "slash/name", "space name", "plain",
        )
        for index, username in enumerate(punctuation, 10):
            self._add_observation(
                username,
                contributor_usernames=(username,),
                observed_at=datetime(2026, 8, 3, tzinfo=UTC)
                + timedelta(seconds=index),
            )
        signal = self.db.get_convergence_signals([self.request_id])[self.request_id]
        self.assertEqual(signal.distinct_peer_count, 5)

    def test_one_five_peer_mosaic_does_not_converge(self) -> None:
        self._add_observation(
            "alice, bob, carol, dave, erin",
            contributor_usernames=("alice", "bob", "carol", "dave", "erin"),
        )
        self.assertEqual(self.db.get_convergence_signals([self.request_id]), {})

    def test_direct_attribution_requires_structured_contributors(self) -> None:
        log_id = self._add_observation(
            "legacy display only",
            contributor_usernames=(),
        )
        row = self.db._execute(
            "SELECT candidate_evidence_direct "
            "FROM download_log WHERE id = %s",
            (log_id,),
        ).fetchone()
        self.assertFalse(row["candidate_evidence_direct"])
        with self.assertRaises(psycopg2.errors.CheckViolation):
            self.db._execute(
                "UPDATE download_log "
                "SET candidate_evidence_direct = TRUE WHERE id = %s",
                (log_id,),
            )

    def test_token_covers_late_link_raw_spread_and_codec_diversity(self) -> None:
        base = datetime(2026, 8, 3, tzinfo=UTC)
        cliffs = [14_760, 14_900, 15_010, 15_120, 15_240]
        codecs = ["flac", "alac", "flac", "wav", "flac"]
        for index, (cliff, codec) in enumerate(
            zip(cliffs, codecs, strict=True), 1,
        ):
            self._add_observation(
                f"peer-{index}", cliff_hz=cliff, codec=codec,
                observed_at=base + timedelta(seconds=index),
            )
        captured = self.db.get_convergence_signals([self.request_id])[self.request_id]
        self.assertEqual(captured.distinct_codec_count, 3)
        self.assertEqual(captured.raw_cliff_min_hz, 14_760)
        self.assertEqual(captured.raw_cliff_max_hz, 15_240)
        self.assertEqual(captured.cliff_spread_hz, 480)

        late_link_id = self._add_observation(
            "late-peer", direct_attribution=False,
            observed_at=base + timedelta(milliseconds=500),
        )
        before_link = self.db.get_convergence_signals(
            [self.request_id],
        )[self.request_id]
        self.assertEqual(before_link.signal_token, captured.signal_token)
        self.assertEqual(
            before_link.latest_qualifying_log_id,
            captured.latest_qualifying_log_id,
        )

        # A separately committed evidence writer changes an older qualifying
        # fact without changing latest_log_id. The opaque token must still
        # reject the captured snapshot.
        assert TEST_DSN is not None
        writer = PipelineDB(TEST_DSN)
        try:
            writer._execute(
                "UPDATE download_log SET candidate_evidence_direct = TRUE "
                "WHERE id = %s",
                (late_link_id,),
            )
            writer.conn.commit()
        finally:
            writer.close()
        after_link = self.db.get_convergence_signals(
            [self.request_id],
        )[self.request_id]
        self.assertEqual(
            after_link.latest_qualifying_log_id,
            captured.latest_qualifying_log_id,
        )
        self.assertNotEqual(after_link.signal_token, captured.signal_token)
        stale = ConvergenceStopService(self.db).stop(
            self.request_id, signal_token=captured.signal_token,
        )
        self.assertEqual(stale.outcome, "stale")
        request = self.db.get_request(self.request_id)
        assert request is not None
        self.assertEqual(request["status"], "wanted")

    def test_token_covers_current_holding_evidence_identity(self) -> None:
        for index in range(5):
            self._add_observation(f"peer-{index}")
        captured = self.db.get_convergence_signals([self.request_id])[self.request_id]

        replacement_current = make_album_quality_evidence(
            mb_release_id="convergence-release",
            source_path="/library/convergence-remeasured",
            files=[AlbumQualityEvidenceFile(
                relative_path="01.mp3",
                size_bytes=44_001,
                mtime_ns=1_700_000_000_000_044_001,
                extension="mp3",
                container="mp3",
                codec="mp3",
            )],
            measurement=AudioQualityMeasurement(format="MP3"),
            v0_metric=AlbumQualityV0Metric(
                subject="source",
                provenance="measured",
                min_bitrate_kbps=220,
                avg_bitrate_kbps=230,
            ),
        )
        self.db.upsert_album_quality_evidence(replacement_current)
        stored = self.db.find_album_quality_evidence(
            mb_release_id=replacement_current.mb_release_id,
            snapshot_fingerprint=replacement_current.snapshot_fingerprint,
        )
        assert stored is not None and stored.id is not None
        self.assertTrue(self.db.set_request_current_evidence(
            self.request_id, stored.id,
        ))

        changed = self.db.get_convergence_signals([self.request_id])[self.request_id]
        self.assertEqual(
            changed.latest_qualifying_log_id,
            captured.latest_qualifying_log_id,
        )
        self.assertNotEqual(changed.signal_token, captured.signal_token)
        result = ConvergenceStopService(self.db).stop(
            self.request_id, signal_token=captured.signal_token,
        )
        self.assertEqual(result.outcome, "stale")
        request = self.db.get_request(self.request_id)
        assert request is not None
        self.assertEqual(request["status"], "wanted")

    def test_waiting_stop_rechecks_current_evidence_on_new_row_version(self) -> None:
        for index in range(5):
            self._add_observation(f"peer-{index}")
        captured = self.db.get_convergence_signals([self.request_id])[self.request_id]

        replacement = make_album_quality_evidence(
            mb_release_id="convergence-release",
            source_path="/library/concurrent-remeasurement",
            files=[AlbumQualityEvidenceFile(
                relative_path="01.mp3",
                size_bytes=55_001,
                mtime_ns=1_700_000_000_000_055_001,
                extension="mp3",
                container="mp3",
                codec="mp3",
            )],
            measurement=AudioQualityMeasurement(format="MP3"),
            v0_metric=AlbumQualityV0Metric(
                subject="source",
                provenance="measured",
                min_bitrate_kbps=220,
                avg_bitrate_kbps=230,
            ),
        )
        self.db.upsert_album_quality_evidence(replacement)
        stored = self.db.find_album_quality_evidence(
            mb_release_id=replacement.mb_release_id,
            snapshot_fingerprint=replacement.snapshot_fingerprint,
        )
        assert stored is not None and stored.id is not None

        assert TEST_DSN is not None
        writer = PipelineDB(TEST_DSN)
        stopper = PipelineDB(TEST_DSN)
        result_queue: queue.Queue[object] = queue.Queue()
        thread: threading.Thread | None = None
        try:
            writer.conn.autocommit = False
            with writer.conn.cursor() as cursor:
                cursor.execute(
                    "UPDATE album_requests SET current_evidence_id = %s "
                    "WHERE id = %s",
                    (stored.id, self.request_id),
                )
            stopper_pid = int(stopper._execute(
                "SELECT pg_backend_pid() AS pid",
            ).fetchone()["pid"])

            def stop_while_locked() -> None:
                try:
                    result_queue.put(ConvergenceStopService(stopper).stop(
                        self.request_id,
                        signal_token=captured.signal_token,
                    ))
                except Exception as exc:  # noqa: BLE001 - relay thread failure
                    result_queue.put(exc)

            thread = threading.Thread(target=stop_while_locked, daemon=True)
            thread.start()
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                waiting = self.db._execute(
                    "SELECT wait_event_type FROM pg_stat_activity "
                    "WHERE pid = %s",
                    (stopper_pid,),
                ).fetchone()
                if waiting is not None and waiting["wait_event_type"] == "Lock":
                    break
                time.sleep(0.01)
            else:
                self.fail("stop statement did not reach the request-row lock")

            writer.conn.commit()
            thread.join(timeout=5)
            self.assertFalse(thread.is_alive())
            outcome = result_queue.get_nowait()
            if isinstance(outcome, Exception):
                raise outcome
            self.assertIsInstance(outcome, StopConvergedSearchResult)
            assert isinstance(outcome, StopConvergedSearchResult)
            self.assertEqual(outcome.outcome, "stale")
            request = self.db.get_request(self.request_id)
            assert request is not None
            self.assertEqual(request["status"], "wanted")
            self.assertEqual(request["current_evidence_id"], stored.id)
        finally:
            if not writer.conn.autocommit:
                writer.conn.rollback()
            if thread is not None and thread.is_alive():
                writer.conn.rollback()
                thread.join(timeout=5)
            writer.close()
            stopper.close()


if __name__ == "__main__":
    unittest.main()
