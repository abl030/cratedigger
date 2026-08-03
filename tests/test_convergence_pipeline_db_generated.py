"""Generated differential checks for PostgreSQL convergence derivation."""

from __future__ import annotations

import unittest
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from hypothesis import given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401 - registers suite/fuzz
from lib.convergence_service import (
    ConvergenceObservation,
    ConvergenceSignal,
    ConvergenceStopService,
    derive_convergence_signal,
)
from lib.pipeline_db import PipelineDB
from lib.quality import (
    AlbumQualityEvidenceFile,
    AlbumQualityV0Metric,
    AudioQualityMeasurement,
)
from tests.helpers import make_album_quality_evidence
from tests.test_pipeline_db import TEST_DSN, make_db, requires_postgres

_BASE = datetime(2026, 8, 3, tzinfo=UTC)


@dataclass(frozen=True)
class AttemptWorld:
    peer_set: str
    cliff_hz: int | None = 15_000
    codec: str = "flac"
    direct: bool = True
    eligible: bool = True
    time_rank: int = 0


@dataclass(frozen=True)
class ConvergenceWorld:
    attempts: tuple[AttemptWorld, ...]


_WORLDS = (
    ConvergenceWorld(tuple(
        AttemptWorld(f"peer-{index}", time_rank=index)
        for index in range(5)
    )),
    ConvergenceWorld(tuple(
        AttemptWorld("same-peer", time_rank=index)
        for index in range(8)
    )),
    ConvergenceWorld((
        AttemptWorld("alice", time_rank=1),
        AttemptWorld("bob", time_rank=2),
        AttemptWorld("carol", time_rank=3),
        AttemptWorld("alice, bob", time_rank=4),
        AttemptWorld("alice, carol", time_rank=5),
    )),
    ConvergenceWorld((
        AttemptWorld("alice", time_rank=1),
        AttemptWorld("bob", time_rank=2),
        AttemptWorld("carol", time_rank=3),
        AttemptWorld("dave, erin", time_rank=4),
        AttemptWorld("alice, bob", time_rank=5),
    )),
    ConvergenceWorld(tuple(
        AttemptWorld(f"crosswalk-{index}", direct=False, time_rank=index)
        for index in range(6)
    )),
    ConvergenceWorld(tuple(
        AttemptWorld(f"peer-{index}", eligible=False, time_rank=index)
        for index in range(6)
    )),
    ConvergenceWorld(tuple(
        AttemptWorld(f"peer-{index}", time_rank=index)
        for index in range(5)
    ) + (AttemptWorld("new-null", cliff_hz=None, time_rank=9),)),
    ConvergenceWorld(tuple(
        AttemptWorld(f"old-{index}", time_rank=index)
        for index in range(6)
    ) + tuple(
        AttemptWorld(f"new-{index}", cliff_hz=16_000, time_rank=10 + index)
        for index in range(4)
    )),
    ConvergenceWorld(tuple(
        AttemptWorld(
            f"peer-{index}",
            cliff_hz=(14_760, 14_900, 15_010, 15_120, 15_240)[index],
            codec=("flac", "alac", "flac", "wav", "flac")[index],
            time_rank=1,
        )
        for index in range(5)
    )),
)


def _signal_facts(signal: ConvergenceSignal | None) -> tuple[object, ...] | None:
    if signal is None:
        return None
    return (
        signal.request_id,
        signal.observation_count,
        signal.distinct_peer_count,
        signal.distinct_candidate_snapshot_count,
        signal.distinct_codec_count,
        signal.cliff_hz,
        signal.raw_cliff_min_hz,
        signal.raw_cliff_max_hz,
        signal.cliff_spread_hz,
        signal.latest_qualifying_log_id,
        signal.first_observed_at,
        signal.latest_observed_at,
    )


def assert_sql_matches_reference(
    sql_signal: ConvergenceSignal | None,
    reference_signal: ConvergenceSignal | None,
) -> None:
    if _signal_facts(sql_signal) != _signal_facts(reference_signal):
        raise AssertionError(
            "PostgreSQL convergence drifted from the independent reference: "
            f"sql={sql_signal!r} reference={reference_signal!r}"
        )


def _seed_current_request(db: PipelineDB) -> int:
    request_id = db.add_request(
        "Generated Convergence Artist",
        "Generated Convergence Album",
        "request",
        mb_release_id="generated-convergence-release",
        status="wanted",
    )
    current = make_album_quality_evidence(
        mb_release_id="generated-convergence-release",
        source_path="/library/generated-convergence",
        measurement=AudioQualityMeasurement(format="MP3"),
        v0_metric=AlbumQualityV0Metric(
            subject="source", provenance="measured",
            min_bitrate_kbps=220, avg_bitrate_kbps=230,
        ),
    )
    db.upsert_album_quality_evidence(current)
    stored = db.find_album_quality_evidence(
        mb_release_id=current.mb_release_id,
        snapshot_fingerprint=current.snapshot_fingerprint,
    )
    assert stored is not None and stored.id is not None
    assert db.set_request_current_evidence(request_id, stored.id)
    return request_id


def _seed_attempt(
    db: PipelineDB,
    request_id: int,
    attempt: AttemptWorld,
) -> ConvergenceObservation:
    log_id = db.log_download(
        request_id,
        soulseek_username=attempt.peer_set,
        filetype=attempt.codec,
        beets_distance=0.05,
        beets_scenario=("strong_match" if attempt.eligible else "high_distance"),
        outcome="rejected",
    )
    evidence = make_album_quality_evidence(
        mb_release_id="generated-convergence-release",
        source_path=f"/candidate/{log_id}",
        files=[AlbumQualityEvidenceFile(
            relative_path=f"01.{attempt.codec}",
            size_bytes=10_000 + log_id,
            mtime_ns=1_700_000_000_000_000_000 + log_id,
            extension=attempt.codec,
            container=attempt.codec,
            codec=attempt.codec,
        )],
        measurement=AudioQualityMeasurement(
            min_bitrate_kbps=800,
            avg_bitrate_kbps=900,
            median_bitrate_kbps=850,
            format=attempt.codec.upper(),
            spectral_grade="genuine",
            spectral_subject="source",
            spectral_provenance="measured",
            cliff_hz=attempt.cliff_hz,
            codec_family="lossless",
            ultrasonic_deficit_db=8.5,
            spectral_measurement_version=2,
        ),
        codec=attempt.codec,
        container=attempt.codec,
        storage_format=attempt.codec.upper(),
        preserve_spectral_measurement_version=True,
    )
    db.upsert_album_quality_evidence(evidence)
    stored = db.find_album_quality_evidence(
        mb_release_id=evidence.mb_release_id,
        snapshot_fingerprint=evidence.snapshot_fingerprint,
    )
    assert stored is not None and stored.id is not None
    db.set_download_log_candidate_evidence(
        log_id, stored.id, direct_attribution=attempt.direct,
    )
    observed_at = _BASE + timedelta(seconds=attempt.time_rank)
    db._execute(
        "UPDATE download_log SET created_at = %s WHERE id = %s",
        (observed_at, log_id),
    )
    db.conn.commit()
    return ConvergenceObservation(
        log_id=log_id,
        peer=attempt.peer_set,
        snapshot_fingerprint=evidence.snapshot_fingerprint,
        codec=attempt.codec,
        cliff_hz=attempt.cliff_hz,
        observed_at=observed_at,
        eligible=attempt.eligible,
        direct_attribution=attempt.direct,
    )


@requires_postgres
class TestGeneratedConvergencePipelineDB(unittest.TestCase):
    @given(world=st.sampled_from(_WORLDS))
    def test_sql_derivation_matches_reference_model(
        self, world: ConvergenceWorld,
    ) -> None:
        db = make_db()
        try:
            request_id = _seed_current_request(db)
            observations = [
                _seed_attempt(db, request_id, attempt)
                for attempt in world.attempts
            ]
            sql_signal = db.get_convergence_signals([request_id]).get(request_id)
            reference = derive_convergence_signal(request_id, observations)
            assert_sql_matches_reference(sql_signal, reference)
            if sql_signal is not None:
                self.assertRegex(sql_signal.signal_token, r"^[0-9a-f]{64}$")
        finally:
            db.close()

    @given(
        peer_set=st.sampled_from(("late", "late, sixth", "alice, bob")),
        cliff_hz=st.sampled_from((14_800, 15_000, 15_200)),
        codec=st.sampled_from(("flac", "alac", "wav")),
    )
    def test_generated_late_link_invalidates_opaque_token(
        self, peer_set: str, cliff_hz: int, codec: str,
    ) -> None:
        db = make_db()
        try:
            request_id = _seed_current_request(db)
            for index in range(5):
                _seed_attempt(db, request_id, AttemptWorld(
                    f"peer-{index}", time_rank=index + 1,
                ))
            late = _seed_attempt(db, request_id, AttemptWorld(
                peer_set, cliff_hz=cliff_hz, codec=codec,
                direct=False, time_rank=0,
            ))
            captured = db.get_convergence_signals([request_id])[request_id]

            assert TEST_DSN is not None
            writer = PipelineDB(TEST_DSN)
            try:
                writer._execute(
                    "UPDATE download_log SET candidate_evidence_direct = TRUE "
                    "WHERE id = %s",
                    (late.log_id,),
                )
                writer.conn.commit()
            finally:
                writer.close()

            changed = db.get_convergence_signals([request_id]).get(request_id)
            if changed is not None:
                self.assertNotEqual(changed.signal_token, captured.signal_token)
            result = ConvergenceStopService(db).stop(
                request_id, signal_token=captured.signal_token,
            )
            self.assertIn(result.outcome, {"stale", "not_converged"})
            request = db.get_request(request_id)
            assert request is not None
            self.assertEqual(request["status"], "wanted")
        finally:
            db.close()


class TestConvergenceDifferentialChecker(unittest.TestCase):
    def test_known_bad_raw_string_peer_counter_is_rejected(self) -> None:
        # Five raw provenance strings contain only three atomic usernames.
        reference = None
        known_bad = ConvergenceSignal(
            request_id=1,
            observation_count=5,
            distinct_peer_count=5,
            distinct_candidate_snapshot_count=5,
            distinct_codec_count=1,
            cliff_hz=15_000,
            raw_cliff_min_hz=15_000,
            raw_cliff_max_hz=15_000,
            cliff_spread_hz=0,
            latest_qualifying_log_id=5,
            first_observed_at=_BASE,
            latest_observed_at=_BASE,
            signal_token="known-bad",
        )
        with self.assertRaises(AssertionError):
            assert_sql_matches_reference(known_bad, reference)

    def test_known_bad_crosswalk_admission_is_rejected(self) -> None:
        reference = None
        known_bad = ConvergenceSignal(
            request_id=1,
            observation_count=6,
            distinct_peer_count=6,
            distinct_candidate_snapshot_count=1,
            distinct_codec_count=1,
            cliff_hz=15_000,
            raw_cliff_min_hz=15_000,
            raw_cliff_max_hz=15_000,
            cliff_spread_hz=0,
            latest_qualifying_log_id=6,
            first_observed_at=_BASE,
            latest_observed_at=_BASE,
            signal_token="known-bad",
        )
        with self.assertRaises(AssertionError):
            assert_sql_matches_reference(known_bad, reference)


if __name__ == "__main__":
    unittest.main()
