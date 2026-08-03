"""Generated differential checks for PostgreSQL convergence derivation."""

from __future__ import annotations

import unittest
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import msgspec
from hypothesis import example, given, settings
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
from tests.test_pipeline_db import make_db, requires_postgres

_BASE = datetime(2026, 8, 3, tzinfo=UTC)
_PEER_IDENTITIES = (
    "alice",
    "Bob",
    "carol",
    "dave",
    "erin",
    "comma,name",
    "semi;colon",
    "slash/name",
    "space name",
)
_CLIFFS = (None, 14_760, 14_900, 15_000, 15_120, 15_240, 15_500, 16_000)
_CODECS = ("flac", "alac", "wav", "ape")


@dataclass(frozen=True)
class AttemptWorld:
    contributor_usernames: tuple[str, ...]
    cliff_hz: int | None = 15_000
    codec: str = "flac"
    direct: bool = True
    eligible: bool = True
    time_rank: int = 0

    @property
    def display_username(self) -> str | None:
        if not self.contributor_usernames:
            return None
        return ", ".join(self.contributor_usernames)


@dataclass(frozen=True)
class ConvergenceWorld:
    attempts: tuple[AttemptWorld, ...]
    replace_current_evidence: bool = False


@st.composite
def attempt_worlds(draw: st.DrawFn) -> AttemptWorld:
    return AttemptWorld(
        contributor_usernames=tuple(draw(st.lists(
            st.sampled_from(_PEER_IDENTITIES),
            min_size=0,
            max_size=5,
            unique=True,
        ))),
        cliff_hz=draw(st.sampled_from(_CLIFFS)),
        codec=draw(st.sampled_from(_CODECS)),
        direct=draw(st.booleans()),
        eligible=draw(st.booleans()),
        time_rank=draw(st.integers(min_value=0, max_value=4)),
    )


@st.composite
def convergence_worlds(draw: st.DrawFn) -> ConvergenceWorld:
    return ConvergenceWorld(
        attempts=tuple(draw(st.lists(
            attempt_worlds(),
            min_size=0,
            max_size=10,
        ))),
        replace_current_evidence=draw(st.booleans()),
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


def _new_current_evidence(*, suffix: str):
    suffix_token = sum((index + 1) * ord(char) for index, char in enumerate(suffix))
    return make_album_quality_evidence(
        mb_release_id="generated-convergence-release",
        source_path=f"/library/generated-convergence-{suffix}",
        files=[AlbumQualityEvidenceFile(
            relative_path="01.mp3",
            size_bytes=50_000 + suffix_token,
            mtime_ns=1_700_000_000_000_000_000 + suffix_token,
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


def _seed_current_request(db: PipelineDB) -> int:
    request_id = db.add_request(
        "Generated Convergence Artist",
        "Generated Convergence Album",
        "request",
        mb_release_id="generated-convergence-release",
        status="wanted",
    )
    current = _new_current_evidence(suffix="initial")
    db.upsert_album_quality_evidence(current)
    stored = db.find_album_quality_evidence(
        mb_release_id=current.mb_release_id,
        snapshot_fingerprint=current.snapshot_fingerprint,
    )
    assert stored is not None and stored.id is not None
    assert db.set_request_current_evidence(request_id, stored.id)
    return request_id


def _replace_current_evidence(
    db: PipelineDB,
    request_id: int,
    *,
    suffix: str,
) -> int:
    current = _new_current_evidence(suffix=suffix)
    db.upsert_album_quality_evidence(current)
    stored = db.find_album_quality_evidence(
        mb_release_id=current.mb_release_id,
        snapshot_fingerprint=current.snapshot_fingerprint,
    )
    assert stored is not None and stored.id is not None
    assert db.set_request_current_evidence(request_id, stored.id)
    return stored.id


def _seed_attempt(
    db: PipelineDB,
    request_id: int,
    attempt: AttemptWorld,
) -> ConvergenceObservation:
    log_id = db.log_download(
        request_id,
        soulseek_username=attempt.display_username,
        contributor_usernames=attempt.contributor_usernames,
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
        log_id,
        stored.id,
        direct_attribution=attempt.direct,
        contributor_usernames=attempt.contributor_usernames,
    )
    observed_at = _BASE + timedelta(seconds=attempt.time_rank)
    db._execute(
        "UPDATE download_log SET created_at = %s WHERE id = %s",
        (observed_at, log_id),
    )
    db.conn.commit()
    return ConvergenceObservation(
        log_id=log_id,
        contributor_usernames=attempt.contributor_usernames,
        snapshot_fingerprint=evidence.snapshot_fingerprint,
        codec=attempt.codec,
        cliff_hz=attempt.cliff_hz,
        observed_at=observed_at,
        eligible=attempt.eligible,
        direct_attribution=(attempt.direct and bool(attempt.contributor_usernames)),
    )


_FIVE_DIRECT = ConvergenceWorld(tuple(
    AttemptWorld((f"peer-{index}",), time_rank=index)
    for index in range(5)
))
_ONE_MOSAIC = ConvergenceWorld((AttemptWorld(
    ("alice", "bob", "carol", "dave", "erin"),
),))


@requires_postgres
class TestGeneratedConvergencePipelineDB(unittest.TestCase):
    @settings(max_examples=24, deadline=None)
    @example(world=_FIVE_DIRECT)
    @example(world=_ONE_MOSAIC)
    @given(world=convergence_worlds())
    def test_sql_derivation_matches_reference_model(
        self,
        world: ConvergenceWorld,
    ) -> None:
        db = make_db()
        try:
            request_id = _seed_current_request(db)
            observations = [
                _seed_attempt(db, request_id, attempt)
                for attempt in world.attempts
            ]
            if world.replace_current_evidence:
                _replace_current_evidence(db, request_id, suffix="replacement")
            sql_signal = db.get_convergence_signals([request_id]).get(request_id)
            reference = derive_convergence_signal(request_id, observations)
            assert_sql_matches_reference(sql_signal, reference)
            if sql_signal is not None:
                self.assertRegex(sql_signal.signal_token, r"^[0-9a-f]{64}$")
        finally:
            db.close()

    @settings(max_examples=12, deadline=None)
    @given(
        mutation=st.sampled_from((
            "late_link", "contributors", "current_evidence", "spectral",
        )),
        punctuation_peer=st.sampled_from(_PEER_IDENTITIES),
    )
    def test_generated_mutation_invalidates_opaque_token(
        self,
        mutation: str,
        punctuation_peer: str,
    ) -> None:
        db = make_db()
        try:
            request_id = _seed_current_request(db)
            observations = [
                _seed_attempt(db, request_id, AttemptWorld(
                    (f"peer-{index}",), time_rank=index + 1,
                ))
                for index in range(5)
            ]
            captured = db.get_convergence_signals([request_id])[request_id]

            if mutation == "late_link":
                late = _seed_attempt(db, request_id, AttemptWorld(
                    (punctuation_peer,), direct=False, time_rank=0,
                ))
                db._execute(
                    "UPDATE download_log SET candidate_evidence_direct = TRUE "
                    "WHERE id = %s",
                    (late.log_id,),
                )
                db.conn.commit()
                observations.append(msgspec.structs.replace(
                    late,
                    direct_attribution=True,
                ))
            elif mutation == "contributors":
                target = observations[-1]
                contributors = tuple(sorted({
                    *target.contributor_usernames,
                    punctuation_peer.lower(),
                }))
                db._execute(
                    "UPDATE download_log "
                    "SET candidate_contributor_usernames = %s "
                    "WHERE id = %s",
                    (list(contributors), target.log_id),
                )
                db.conn.commit()
                observations[-1] = msgspec.structs.replace(
                    target,
                    contributor_usernames=contributors,
                )
            elif mutation == "current_evidence":
                _replace_current_evidence(db, request_id, suffix=punctuation_peer)
            else:
                target = observations[-1]
                db._execute(
                    "UPDATE album_quality_evidence "
                    "SET cliff_hz = %s, codec = %s "
                    "WHERE id = (SELECT candidate_evidence_id "
                    "FROM download_log WHERE id = %s)",
                    (16_000, "wav", target.log_id),
                )
                db.conn.commit()
                observations[-1] = msgspec.structs.replace(
                    target,
                    cliff_hz=16_000,
                    codec="wav",
                )

            changed = db.get_convergence_signals([request_id]).get(request_id)
            reference = derive_convergence_signal(request_id, observations)
            assert_sql_matches_reference(changed, reference)
            if changed is not None:
                self.assertNotEqual(changed.signal_token, captured.signal_token)
            result = ConvergenceStopService(db).stop(
                request_id,
                signal_token=captured.signal_token,
            )
            self.assertIn(result.outcome, {"stale", "not_converged"})
            request = db.get_request(request_id)
            assert request is not None
            self.assertEqual(request["status"], "wanted")
        finally:
            db.close()


class TestConvergenceDifferentialChecker(unittest.TestCase):
    def test_known_bad_one_mosaic_observation_is_rejected(self) -> None:
        reference = derive_convergence_signal(1, [ConvergenceObservation(
            log_id=1,
            contributor_usernames=("alice", "bob", "carol", "dave", "erin"),
            snapshot_fingerprint="snapshot",
            codec="flac",
            cliff_hz=15_000,
            observed_at=_BASE,
            eligible=True,
        )])
        known_bad = ConvergenceSignal(
            request_id=1,
            observation_count=1,
            distinct_peer_count=5,
            distinct_candidate_snapshot_count=1,
            distinct_codec_count=1,
            cliff_hz=15_000,
            raw_cliff_min_hz=15_000,
            raw_cliff_max_hz=15_000,
            cliff_spread_hz=0,
            latest_qualifying_log_id=1,
            first_observed_at=_BASE,
            latest_observed_at=_BASE,
            signal_token="known-bad",
        )
        self.assertIsNone(reference)
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
