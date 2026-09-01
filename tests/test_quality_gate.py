"""Focused contracts for the post-import quality-gate adapter and write plan."""

from __future__ import annotations

import unittest
from unittest.mock import patch

import msgspec

from lib.dispatch import quality_gate as quality_gate_module
from lib.dispatch.quality_gate import _check_quality_gate_core, load_quality_gate_state
from lib.dispatch.types import QualityGateState
from lib.quality import (
    AlbumQualityV0Metric,
    AudioQualityMeasurement,
    QualityRankConfig,
    TargetQualityContract,
)
from lib.quality.decisions import QualityGateDecision
from tests.evidence_helpers import make_album_quality_evidence
from tests.fakes import FakePipelineDB
from tests.helpers import make_download_file, make_request_row


class TestEvidenceUnavailablePlan(unittest.TestCase):
    def test_reopens_only_the_imported_request_without_blame(self) -> None:
        plan = quality_gate_module._evidence_unavailable_plan()

        self.assertEqual(plan.transition.target_status, "wanted")
        self.assertEqual(plan.transition.from_status, "imported")
        self.assertEqual(
            dict(plan.transition.fields),
            {"search_filetype_override": None},
        )
        self.assertEqual(plan.denylists, ())
        self.assertIs(plan.successful_terminal_acceptance, False)

class TestLoadQualityGateStateContracts(unittest.TestCase):
    @staticmethod
    def _linked_db(*, evidence=None, request_mbid: str = "release-42") -> FakePipelineDB:
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            status="imported",
            mb_release_id=request_mbid,
        ))
        if evidence is None:
            evidence = make_album_quality_evidence(mb_release_id=request_mbid)
        db.upsert_album_quality_evidence(evidence)
        persisted = db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert persisted is not None and persisted.id is not None
        db.set_request_current_evidence(42, persisted.id)
        return db

    def test_recovers_release_identity_from_the_exact_request(self) -> None:
        db = self._linked_db()
        request_ids: list[int] = []
        real_get_request = db.get_request

        def record_get_request(request_id: int):
            request_ids.append(request_id)
            return real_get_request(request_id)

        with patch.object(db, "get_request", side_effect=record_get_request):
            state = load_quality_gate_state(request_id=42, db=db)

        self.assertIsNotNone(state)
        self.assertEqual(request_ids, [42])

    def test_request_without_release_identity_has_no_gate_state(self) -> None:
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            status="imported",
            mb_release_id=None,
        ))

        with patch.object(
            db,
            "get_request_current_evidence_id",
            side_effect=AssertionError("identity-free request must stop before evidence"),
        ) as current_evidence_id:
            self.assertIsNone(load_quality_gate_state(request_id=42, db=db))

        current_evidence_id.assert_not_called()

    def test_requires_present_matching_current_evidence(self) -> None:
        absent = FakePipelineDB()
        absent.seed_request(make_request_row(
            id=42,
            status="imported",
            mb_release_id="release-42",
        ))
        self.assertIsNone(load_quality_gate_state(
            request_id=42,
            db=absent,
            mb_id="release-42",
        ))

        mismatched = self._linked_db(evidence=make_album_quality_evidence(
            mb_release_id="different-release",
        ))
        self.assertIsNone(load_quality_gate_state(
            request_id=42,
            db=mismatched,
            mb_id="release-42",
        ))

        stale_evidence = msgspec.structs.replace(
            make_album_quality_evidence(mb_release_id="release-42"),
            lineage_version=1,
        )
        stale = self._linked_db(evidence=stale_evidence)
        self.assertIsNone(load_quality_gate_state(
            request_id=42,
            db=stale,
            mb_id="release-42",
        ))

    def test_interprets_spectral_with_the_linked_measurement_context(self) -> None:
        measurement = AudioQualityMeasurement(
            min_bitrate_kbps=320,
            avg_bitrate_kbps=320,
            median_bitrate_kbps=320,
            format="MP3",
            is_cbr=True,
            spectral_grade="likely_transcode",
            spectral_bitrate_kbps=128,
            spectral_subject="installed",
            spectral_provenance="measured",
            codec_family="mp3",
            cliff_hz=16_000,
        )
        db = self._linked_db(evidence=make_album_quality_evidence(
            mb_release_id="release-42",
            measurement=measurement,
            storage_format="MP3",
        ))

        state = load_quality_gate_state(
            request_id=42,
            db=db,
            mb_id="release-42",
        )

        self.assertIsNotNone(state)
        assert state is not None and state.spectral_context is not None
        self.assertEqual(state.spectral_context.codec_family, "mp3")
        self.assertEqual(state.spectral_context.cliff_hz, 16_000)
        self.assertEqual(state.spectral_context.spectral_subject, "installed")
        self.assertEqual(state.measurement.spectral_bitrate_kbps, 160)

    def test_spectral_clamp_receives_the_linked_container_floor(self) -> None:
        measurement = AudioQualityMeasurement(
            min_bitrate_kbps=207,
            avg_bitrate_kbps=221,
            median_bitrate_kbps=219,
            format="MP3",
            is_cbr=False,
        )
        db = self._linked_db(evidence=make_album_quality_evidence(
            mb_release_id="release-42",
            measurement=measurement,
        ))
        container_floors: list[int | None] = []

        def record_floor(container_bitrate, spectral):
            del spectral
            container_floors.append(container_bitrate)
            return container_bitrate

        with patch.object(
            quality_gate_module,
            "compute_effective_override_bitrate",
            side_effect=record_floor,
        ):
            state = load_quality_gate_state(
                request_id=42,
                db=db,
                mb_id="release-42",
            )

        self.assertIsNotNone(state)
        self.assertEqual(container_floors, [207])

    def test_exposes_only_source_v0_metric_as_source_context(self) -> None:
        source_db = self._linked_db(evidence=make_album_quality_evidence(
            mb_release_id="release-42",
            v0_metric=AlbumQualityV0Metric(
                subject="source",
                provenance="carried",
                min_bitrate_kbps=171,
                avg_bitrate_kbps=181,
            ),
        ))
        source = load_quality_gate_state(
            request_id=42,
            db=source_db,
            mb_id="release-42",
        )
        self.assertIsNotNone(source)
        assert source is not None
        self.assertEqual(source.source_v0_avg_bitrate_kbps, 181)

        installed_db = self._linked_db(evidence=make_album_quality_evidence(
            mb_release_id="release-42",
            v0_metric=AlbumQualityV0Metric(
                subject="installed",
                provenance="measured",
                min_bitrate_kbps=171,
                avg_bitrate_kbps=181,
            ),
        ))
        installed = load_quality_gate_state(
            request_id=42,
            db=installed_db,
            mb_id="release-42",
        )
        self.assertIsNotNone(installed)
        assert installed is not None
        self.assertIsNone(installed.source_v0_avg_bitrate_kbps)


class TestQualityGatePlanContracts(unittest.TestCase):
    @staticmethod
    def _state(*, proof: bool = False) -> QualityGateState:
        return QualityGateState(
            measurement=AudioQualityMeasurement(
                min_bitrate_kbps=320,
                avg_bitrate_kbps=320,
                median_bitrate_kbps=320,
                format="MP3",
                is_cbr=True,
                spectral_grade="genuine",
                spectral_subject="installed",
                spectral_provenance="measured",
            ),
            verified_lossless_proof=proof,
        )

    @staticmethod
    def _decision(decision: QualityGateDecision):
        def decide(
            current: AudioQualityMeasurement,
            cfg: QualityRankConfig | None = None,
            *,
            target_contract: TargetQualityContract | None = None,
            verified_lossless_proof: bool = False,
        ) -> QualityGateDecision:
            del current, target_contract, verified_lossless_proof
            if not isinstance(cfg, QualityRankConfig):
                raise TypeError("quality decision requires concrete rank config")
            return decision

        return decide

    @staticmethod
    def _db(*, override: str | None = None) -> FakePipelineDB:
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            status="imported",
            search_filetype_override=override,
        ))
        return db

    def test_lossless_retry_plan_fences_and_persists_exact_peer_policy(self) -> None:
        db = self._db()
        plan = _check_quality_gate_core(
            mb_id="release-42",
            label="Artist - Album",
            request_id=42,
            files=[
                make_download_file(username="peer-b", filename="02.mp3"),
                make_download_file(username="peer-a", filename="01.mp3"),
            ],
            db=db,
            state_loader=lambda **_kwargs: self._state(),
            quality_decision_fn=self._decision("requeue_lossless"),
        )

        self.assertIsNotNone(plan)
        assert plan is not None
        expected_reason = (
            "quality gate: transparent installed copy independently "
            "verified genuine; continuing lossless-only search"
        )
        self.assertEqual(plan.transition.target_status, "wanted")
        self.assertEqual(plan.transition.from_status, "imported")
        self.assertEqual(dict(plan.transition.fields), {
            "search_filetype_override": "lossless",
            "min_bitrate": 320,
        })
        self.assertIs(plan.successful_terminal_acceptance, False)
        self.assertEqual(
            [(entry.username, entry.reason) for entry in plan.denylists],
            [("peer-a", expected_reason), ("peer-b", expected_reason)],
        )
        self.assertEqual(
            [(entry.request_id, entry.username, entry.reason) for entry in db.denylist],
            [
                (42, "peer-a", expected_reason),
                (42, "peer-b", expected_reason),
            ],
        )

    def test_full_tier_retry_reason_names_the_missing_proof(self) -> None:
        db = self._db()
        plan = _check_quality_gate_core(
            mb_id="release-42",
            label="Artist - Album",
            request_id=42,
            files=[make_download_file(username="peer", filename="01.mp3")],
            db=db,
            apply=False,
            state_loader=lambda **_kwargs: self._state(),
            quality_decision_fn=self._decision("requeue_upgrade"),
        )

        self.assertIsNotNone(plan)
        assert plan is not None
        self.assertEqual(
            [(entry.username, entry.reason) for entry in plan.denylists],
            [(
                "peer",
                "quality gate: no verified-lossless proof; continuing full-tier search",
            )],
        )

    def test_terminal_acceptance_fences_and_marks_the_plan(self) -> None:
        db = self._db()
        plan = _check_quality_gate_core(
            mb_id="release-42",
            label="Artist - Album",
            request_id=42,
            files=[],
            db=db,
            apply=False,
            state_loader=lambda **_kwargs: self._state(proof=True),
            quality_decision_fn=self._decision("accept"),
        )

        self.assertIsNotNone(plan)
        assert plan is not None
        self.assertEqual(plan.transition.target_status, "imported")
        self.assertEqual(plan.transition.from_status, "imported")
        self.assertEqual(dict(plan.transition.fields), {
            "min_bitrate": 320,
            "search_filetype_override": None,
        })
        self.assertEqual(plan.denylists, ())
        self.assertIs(plan.successful_terminal_acceptance, True)

if __name__ == "__main__":
    unittest.main()
