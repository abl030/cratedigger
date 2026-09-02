"""Grouped contracts for the importer evidence authorization boundary."""

from __future__ import annotations

import contextlib
import unittest
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import ANY, MagicMock

import msgspec

import lib.beets_db as beets_db_module
import lib.dispatch.evidence_gate as evidence_gate_module
from lib.config import CratediggerConfig
from lib.dispatch.evidence_gate import (
    _download_info_from_candidate_evidence,
    _exact_linked_refresh_result,
    _load_evidence_import_gate,
    _refresh_current_evidence_after_import,
    _requeue_import_job_to_preview,
    _write_album_sidecar_after_import,
    _write_quality_evidence_action_file,
)
from lib.dispatch.types import EvidenceImportGate
from lib.import_evidence import (
    ActionEvidenceProvenance,
    CandidateEvidenceActionResult,
    CurrentEvidenceActionResult,
)
from lib.quality import (
    AlbumQualityV0Metric,
    AudioQualityMeasurement,
    QualityRankConfig,
    SpectralAnalysisDetail,
    VerifiedLosslessProof,
)
from lib.quality_evidence import EvidenceBuildResult
from tests.evidence_helpers import make_album_quality_evidence
from tests.fakes import FakePipelineDB

MBID = "12345678-1234-1234-1234-123456789abc"


@contextlib.contextmanager
def _swap(module, name: str, replacement):
    original = getattr(module, name)
    setattr(module, name, replacement)
    try:
        yield replacement
    finally:
        setattr(module, name, original)


class _EvidenceDB(FakePipelineDB):
    def __init__(
        self,
        *,
        linked_id: int | None = None,
        linked=None,
        link_error: Exception | None = None,
        load_error: Exception | None = None,
        job=None,
        requeue_result=None,
        requeue_error: Exception | None = None,
    ) -> None:
        super().__init__()
        self._linked_id = linked_id
        self._linked = linked
        self._link_error = link_error
        self._load_error = load_error
        self._job = job
        self._requeue_result = requeue_result
        self._requeue_error = requeue_error
        self.link_calls = 0
        self.load_calls = 0
        self.requeue_calls: list[tuple[int, str, object]] = []

    def get_request_current_evidence_id(self, request_id: int) -> int | None:
        self.link_calls += 1
        if self._link_error is not None:
            raise self._link_error
        return self._linked_id

    def load_album_quality_evidence_by_id(self, evidence_id: int | None):
        self.load_calls += 1
        if self._load_error is not None:
            raise self._load_error
        return self._linked

    def get_import_job(self, job_id: int):
        return self._job

    def requeue_import_job_for_preview(
        self, job_id: int, *, reason: str, expected_execution_lease=None
    ):
        self.requeue_calls.append((job_id, reason, expected_execution_lease))
        if self._requeue_error is not None:
            raise self._requeue_error
        return self._requeue_result


def _candidate_result(*, available: bool = True) -> CandidateEvidenceActionResult:
    return CandidateEvidenceActionResult(
        evidence=make_album_quality_evidence(mb_release_id=MBID) if available else None,
        provenance=ActionEvidenceProvenance(
            candidate_status="candidate-sentinel",
            snapshot_guard="candidate-snapshot-sentinel",
            fallback_reason="candidate-reason-sentinel",
            fail_closed=not available,
        ),
    )


def _current_result(*, available: bool = True) -> CurrentEvidenceActionResult:
    return CurrentEvidenceActionResult(
        evidence=make_album_quality_evidence(mb_release_id=MBID) if available else None,
        provenance=ActionEvidenceProvenance(
            current_status="current-sentinel",
            snapshot_guard="current-snapshot-sentinel",
            fallback_reason="current-reason-sentinel",
            installed_path="/library/sentinel",
            fail_closed=not available,
        ),
    )


class TestEvidenceGateLoadingContracts(unittest.TestCase):
    def _load(
        self,
        candidate: CandidateEvidenceActionResult,
        current: CurrentEvidenceActionResult | None = None,
        *,
        use_default_current: bool = True,
        **kwargs,
    ):
        loader = MagicMock(
            return_value=_current_result() if use_default_current else current
        )
        gate = _load_evidence_import_gate(
            MagicMock(),
            request_id=41,
            mb_release_id=MBID,
            path="/candidate",
            quality_ranks=kwargs.pop("quality_ranks", None),
            candidate_import_job_id=kwargs.pop("candidate_import_job_id", 7),
            candidate_download_log_id=kwargs.pop("candidate_download_log_id", None),
            prevalidated_candidate_result=candidate,
            current_evidence_loader=loader,
            **kwargs,
        )
        return gate, loader

    def test_candidate_address_presence_matrix(self) -> None:
        candidate = _candidate_result()
        for job_id, log_id, expected_calls in (
            (None, None, 0),
            (7, None, 1),
            (None, 8, 1),
            (7, 8, 1),
        ):
            with self.subTest(job_id=job_id, log_id=log_id):
                gate, loader = self._load(
                    candidate,
                    candidate_import_job_id=job_id,
                    candidate_download_log_id=log_id,
                )
                self.assertEqual(loader.call_count, expected_calls)
                if expected_calls == 0:
                    self.assertEqual(gate, EvidenceImportGate())
                else:
                    self.assertIs(gate.candidate, candidate.evidence)

    def test_unavailable_candidate_preserves_complete_provenance(self) -> None:
        candidate = _candidate_result(available=False)
        gate, loader = self._load(candidate)
        self.assertEqual(loader.call_count, 0)
        self.assertEqual(
            gate,
            EvidenceImportGate(
                candidate=None,
                candidate_status="candidate-sentinel",
                candidate_reason="candidate-reason-sentinel",
                snapshot_guard="candidate-snapshot-sentinel",
            ),
        )

    def test_authoritative_current_absence_is_missing_not_failed(self) -> None:
        candidate = _candidate_result()
        gate, _loader = self._load(candidate, use_default_current=False)
        self.assertEqual(
            gate,
            EvidenceImportGate(
                current=None,
                candidate=candidate.evidence,
                candidate_status="candidate-sentinel",
                candidate_reason="candidate-reason-sentinel",
                current_status="missing",
                current_reason="album not in beets",
                snapshot_guard="candidate-snapshot-sentinel",
            ),
        )

    def test_rank_policy_and_storage_authority_reach_current_loader(self) -> None:
        candidate = _candidate_result()
        ranks = QualityRankConfig.defaults()
        _gate, loader = self._load(
            candidate,
            quality_ranks=ranks,
            beets_library_db_path="/scratch/library.db",
            beets_library_root="/scratch/library",
        )
        loader.assert_called_once_with(
            ANY,
            request_id=41,
            mb_release_id=MBID,
            quality_ranks=ranks,
            beets_library_db_path="/scratch/library.db",
            beets_library_root="/scratch/library",
        )

    def test_fresh_have_failure_matrix_and_ready_control(self) -> None:
        candidate = _candidate_result()
        cases = (
            (None, "attempt returned no installed HAVE spectral result"),
            (SpectralAnalysisDetail(attempted=False), "attempt did not run installed HAVE spectral analysis"),
            (SpectralAnalysisDetail(attempted=True, error="analyser exploded"), "analyser exploded"),
            (SpectralAnalysisDetail(attempted=True, grade=None), "attempt did not produce a usable installed HAVE spectral grade"),
            (SpectralAnalysisDetail(attempted=True, grade="error"), "attempt did not produce a usable installed HAVE spectral grade"),
        )
        for detail, reason in cases:
            with self.subTest(detail=detail):
                gate, _loader = self._load(
                    candidate,
                    attempt_have_audit_available=True,
                    attempt_existing_spectral=detail,
                )
                self.assertEqual(
                    gate,
                    EvidenceImportGate(
                        current=None,
                        candidate=candidate.evidence,
                        candidate_status="candidate-sentinel",
                        candidate_reason="candidate-reason-sentinel",
                        current_status="failed",
                        current_reason=reason,
                        current_path="/library/sentinel",
                        current_snapshot_guard="current-snapshot-sentinel",
                        snapshot_guard="candidate-snapshot-sentinel",
                    ),
                )
        usable = SpectralAnalysisDetail(attempted=True, grade="genuine")
        gate, _loader = self._load(
            candidate,
            attempt_have_audit_available=True,
            attempt_existing_spectral=usable,
        )
        self.assertIsNotNone(gate.current)
        self.assertEqual(gate.current_status, "current-sentinel")
        self.assertEqual(gate.current_reason, "current-reason-sentinel")
        self.assertEqual(gate.current_path, "/library/sentinel")
        self.assertEqual(gate.current_snapshot_guard, "current-snapshot-sentinel")
        self.assertEqual(gate.candidate_status, "candidate-sentinel")
        self.assertEqual(gate.candidate_reason, "candidate-reason-sentinel")
        self.assertEqual(gate.snapshot_guard, "candidate-snapshot-sentinel")


class TestEvidenceAuditProjection(unittest.TestCase):
    def test_rich_candidate_projects_every_download_audit_field(self) -> None:
        measurement = AudioQualityMeasurement(
            min_bitrate_kbps=321,
            format="MEASURED",
            is_cbr=True,
            spectral_grade="marginal",
            spectral_bitrate_kbps=192,
        )
        metric = AlbumQualityV0Metric(subject="source", avg_bitrate_kbps=251)
        candidate = make_album_quality_evidence(
            mb_release_id=MBID,
            measurement=measurement,
            storage_format="STORAGE",
            container="CONTAINER",
            codec="CODEC",
            v0_metric=metric,
        )
        info = _download_info_from_candidate_evidence(
            candidate,
            username="primary",
            contributor_usernames=("one", "two"),
        )
        self.assertEqual(info.username, "primary")
        self.assertEqual(info.contributor_usernames, ("one", "two"))
        self.assertEqual(info.filetype, "STORAGE")
        self.assertEqual(info.bitrate, 321_000)
        self.assertIs(info.is_vbr, False)
        self.assertIsNotNone(info.download_spectral)
        assert info.download_spectral is not None
        self.assertEqual(info.download_spectral.grade, "marginal")
        self.assertEqual(info.download_spectral.bitrate_kbps, 192)
        self.assertIsNotNone(info.v0_probe)
        assert info.v0_probe is not None
        self.assertEqual(info.v0_probe.kind, "lossless_source_v0")
        self.assertEqual(info.v0_probe.avg_bitrate_kbps, 251)

    def test_format_precedence_and_nullable_bitrate(self) -> None:
        for measurement, storage, container, codec, expected in (
            (AudioQualityMeasurement(format="M"), "S", "C", "K", "S"),
            (AudioQualityMeasurement(format="M"), None, "C", "K", "M"),
            (AudioQualityMeasurement(), None, "C", "K", "C"),
            (AudioQualityMeasurement(), None, None, "K", "K"),
        ):
            with self.subTest(expected=expected):
                candidate = make_album_quality_evidence(
                    mb_release_id=MBID,
                    measurement=measurement,
                    storage_format=storage,
                    container=container,
                    codec=codec,
                )
                info = _download_info_from_candidate_evidence(candidate, username=None)
                self.assertEqual(info.filetype, expected)
                if measurement.min_bitrate_kbps is None:
                    self.assertIsNone(info.bitrate)


class TestEvidenceActionPayload(unittest.TestCase):
    def test_real_tempfile_round_trip_preserves_authorization_payload(self) -> None:
        candidate = make_album_quality_evidence(mb_release_id=MBID)
        current = make_album_quality_evidence(mb_release_id=MBID, source_path="/current")
        gate = EvidenceImportGate(
            candidate=candidate,
            current=current,
            candidate_status="candidate-status",
            candidate_reason="candidate-reason",
            current_status="current-status",
            snapshot_guard="snapshot-status",
        )
        decision: dict[str, object] = {"stage2_import": "import_upgrade", "imported": True}
        path = _write_quality_evidence_action_file(
            candidate=candidate,
            current=current,
            decision=decision,
            target_format="MP3 V0",
            verified_lossless_target="FLAC",
            gate=gate,
        )
        try:
            from lib.quality import QualityEvidenceActionPayload

            with open(path, "rb") as handle:
                payload = msgspec.json.decode(
                    handle.read(), type=QualityEvidenceActionPayload
                )
            self.assertEqual(payload.candidate, candidate)
            self.assertEqual(payload.current, current)
            self.assertEqual(payload.decision, decision)
            self.assertEqual(payload.decision_name, "import_upgrade")
            self.assertEqual(payload.target_format, "MP3 V0")
            self.assertEqual(payload.verified_lossless_target, "FLAC")
            self.assertEqual(payload.provenance.candidate_status, "candidate-status")
            self.assertEqual(payload.provenance.current_status, "current-status")
            self.assertEqual(payload.provenance.snapshot_status, "snapshot-status")
            self.assertEqual(payload.provenance.fallback_reason, "candidate-reason")
        finally:
            from lib.evidence_action_file import remove_quality_evidence_action_file

            remove_quality_evidence_action_file(path)


class TestExactLinkedRefresh(unittest.TestCase):
    def _evidence(self, *, row_id: int | None = 12, mbid: str = MBID, source_path: str = "/source"):
        return msgspec.structs.replace(
            make_album_quality_evidence(mb_release_id=mbid, source_path=source_path),
            id=row_id,
        )

    def test_only_ready_evidence_queries_link(self) -> None:
        for status, evidence, expected_calls in (
            ("ready", self._evidence(), 1),
            ("ready", None, 0),
            ("failed", self._evidence(), 0),
            ("failed", None, 0),
        ):
            with self.subTest(status=status, evidence=evidence):
                db = _EvidenceDB(linked_id=12, linked=evidence)
                result = EvidenceBuildResult(evidence, status, "original-reason")
                actual = _exact_linked_refresh_result(
                    db, request_id=41, mb_release_id=MBID, result=result
                )
                self.assertEqual(db.link_calls, expected_calls)
                if expected_calls == 0:
                    self.assertIs(actual, result)

    def test_exact_link_guard_matrix(self) -> None:
        expected = self._evidence()
        variants = (
            (None, None),
            (12, None),
            (12, self._evidence(row_id=None)),
            (12, self._evidence(mbid="other-release")),
            (
                12,
                msgspec.structs.replace(
                    self._evidence(), snapshot_fingerprint="different-snapshot"
                ),
            ),
        )
        for linked_id, linked in variants:
            with self.subTest(linked_id=linked_id, linked=linked):
                db = _EvidenceDB(linked_id=linked_id, linked=linked)
                actual = _exact_linked_refresh_result(
                    db,
                    request_id=41,
                    mb_release_id=MBID,
                    result=EvidenceBuildResult(expected, "ready"),
                )
                self.assertEqual(
                    actual,
                    EvidenceBuildResult(
                        None,
                        "stale_request",
                        "post-import evidence is not the exact linked current snapshot",
                    ),
                )
        db = _EvidenceDB(linked_id=12, linked=expected)
        self.assertEqual(
            _exact_linked_refresh_result(
                db,
                request_id=41,
                mb_release_id=MBID,
                result=EvidenceBuildResult(expected, "ready"),
            ),
            EvidenceBuildResult(expected, "ready"),
        )

    def test_each_link_lookup_failure_becomes_typed_failure(self) -> None:
        for failing_method in ("get_request_current_evidence_id", "load_album_quality_evidence_by_id"):
            with self.subTest(failing_method=failing_method):
                db = _EvidenceDB(
                    linked_id=12,
                    linked=self._evidence(),
                    link_error=(RuntimeError("lookup exploded") if failing_method == "get_request_current_evidence_id" else None),
                    load_error=(RuntimeError("lookup exploded") if failing_method == "load_album_quality_evidence_by_id" else None),
                )
                self.assertEqual(
                    _exact_linked_refresh_result(
                        db,
                        request_id=41,
                        mb_release_id=MBID,
                        result=EvidenceBuildResult(self._evidence(), "ready"),
                    ),
                    EvidenceBuildResult(None, "failed", "RuntimeError: lookup exploded"),
                )


class TestRefreshFailureContracts(unittest.TestCase):
    @contextlib.contextmanager
    def _patched_beets(self, resolution):
        handle = MagicMock()
        handle.__enter__.return_value.resolve_current_release.return_value = resolution
        with _swap(beets_db_module, "open_beets_db", MagicMock(return_value=handle)):
            yield handle

    def test_invalid_identity_fails_before_beets_open(self) -> None:
        opened = MagicMock()
        with _swap(beets_db_module, "release_identity_for_lookup", MagicMock(return_value=None)), _swap(
            beets_db_module, "open_beets_db", opened
        ):
            result = _refresh_current_evidence_after_import(
                MagicMock(), request_id=41, mb_release_id="invalid", quality_ranks=None
            )
        self.assertEqual(
            result,
            EvidenceBuildResult(None, "failed", "invalid exact release identity 'invalid'"),
        )
        opened.assert_called_once()
        opened.return_value.__enter__.assert_not_called()

    def test_candidate_identity_mismatch_stops_before_resolution(self) -> None:
        candidate = make_album_quality_evidence(mb_release_id="other-release")
        handle = MagicMock()
        with _swap(beets_db_module, "release_identity_for_lookup", MagicMock(return_value=MagicMock())), _swap(
            beets_db_module, "exact_release_identity_matches", MagicMock(return_value=False)
        ), _swap(beets_db_module, "open_beets_db", MagicMock(return_value=handle)):
            result = _refresh_current_evidence_after_import(
                MagicMock(),
                request_id=41,
                mb_release_id=MBID,
                quality_ranks=None,
                source_candidate=candidate,
            )
        self.assertEqual(
            result,
            EvidenceBuildResult(
                None,
                "identity_mismatch",
                "candidate evidence exact release identity does not match import",
            ),
        )
        handle.__enter__.assert_not_called()

    def test_missing_ambiguous_and_unusable_current_are_distinct(self) -> None:
        from lib.beets_db import CurrentBeetsAmbiguous, CurrentBeetsMissing

        identity = MagicMock()
        cases = (
            (
                CurrentBeetsMissing(identity),
                EvidenceBuildResult(None, "empty_current", "album not in beets"),
            ),
            (
                CurrentBeetsAmbiguous(identity, (3, 9), "multiple_matches"),
                EvidenceBuildResult(
                    None,
                    "ambiguous_current",
                    "ambiguous current Beets authority: multiple_matches; album_ids=(3, 9)",
                ),
            ),
        )
        for resolution, expected in cases:
            album_info = MagicMock()
            with self.subTest(resolution=resolution), _swap(
                beets_db_module, "release_identity_for_lookup", MagicMock(return_value=identity)
            ), self._patched_beets(resolution), _swap(
                beets_db_module, "album_info_from_current", album_info
            ):
                result = _refresh_current_evidence_after_import(
                    MagicMock(), request_id=41, mb_release_id=MBID, quality_ranks=None
                )
                self.assertEqual(result, expected)
                album_info.assert_not_called()

        unique = SimpleNamespace(album_path="/library/unique")
        with _swap(beets_db_module, "release_identity_for_lookup", MagicMock(return_value=identity)), self._patched_beets(unique), _swap(
            beets_db_module, "album_info_from_current", MagicMock(return_value=None)
        ):
            result = _refresh_current_evidence_after_import(
                MagicMock(), request_id=41, mb_release_id=MBID, quality_ranks=None
            )
        self.assertEqual(
            result,
            EvidenceBuildResult(
                None,
                "failed",
                "unique current Beets album has no usable bitrate metadata",
                current_album_path="/library/unique",
            ),
        )

    def test_legacy_proof_attribution_matrix(self) -> None:
        from lib.quality import ImportResult

        identity = MagicMock()
        unique = SimpleNamespace(album_path="/library/unique")
        album_info = MagicMock()
        proof_none = VerifiedLosslessProof(
            provenance="measured", source="none-source", classifier="test"
        )
        proof_installing = VerifiedLosslessProof(
            provenance="measured", source="installing-source", classifier="test"
        )
        decisions = (
            (None, proof_none, False),
            ("import_upgrade", proof_installing, False),
            ("preflight_existing", None, True),
        )
        for decision, expected_proof, expected_preserve in decisions:
            backfill = MagicMock(return_value=EvidenceBuildResult(None, "failed", "sentinel"))
            exact_link = MagicMock(return_value=EvidenceBuildResult(None, "failed", "linked-sentinel"))
            db = _EvidenceDB()
            with self.subTest(decision=decision), _swap(
                beets_db_module, "release_identity_for_lookup", MagicMock(return_value=identity)
            ), self._patched_beets(unique), _swap(
                beets_db_module, "album_info_from_current", MagicMock(return_value=album_info)
            ), _swap(
                evidence_gate_module, "backfill_current_evidence_from_album_info", backfill
            ), _swap(
                evidence_gate_module, "_exact_linked_refresh_result", exact_link
            ):
                import_result = ImportResult(
                    decision=decision,
                    verified_lossless_proof=(
                        proof_none if decision is None else proof_installing
                    ),
                )
                _refresh_current_evidence_after_import(
                    db,
                    request_id=41,
                    mb_release_id=MBID,
                    quality_ranks=None,
                    import_result=import_result,
                )
                self.assertEqual(
                    backfill.call_args.kwargs["verified_lossless_proof"], expected_proof
                )
                self.assertIs(
                    backfill.call_args.kwargs[
                        "preserve_existing_verified_lossless_proof"
                    ],
                    expected_preserve,
                )
                exact_link.assert_called_once_with(
                    db,
                    request_id=41,
                    mb_release_id=MBID,
                    result=EvidenceBuildResult(None, "failed", "sentinel"),
                )


class TestRequeueContracts(unittest.TestCase):
    def test_no_job_id_has_exact_durable_outcome(self) -> None:
        result = _requeue_import_job_to_preview(
            MagicMock(), import_job_id=None, reason="stale snapshot"
        )
        self.assertIs(result.success, False)
        self.assertEqual(result.code, "requeue_failed")
        self.assertEqual(
            result.message,
            "Candidate quality evidence unavailable at import time: stale snapshot (no import_job_id; cannot requeue)",
        )

    def test_null_created_at_and_execution_fence_are_forwarded(self) -> None:
        db = _EvidenceDB(
            job=SimpleNamespace(created_at=None),
            requeue_result=SimpleNamespace(attempts=2),
        )
        lease = MagicMock(name="lease")
        result = _requeue_import_job_to_preview(
            db,
            import_job_id=7,
            reason="stale snapshot",
            expected_execution_lease=lease,
        )
        self.assertEqual(
            db.requeue_calls, [(7, "stale snapshot", lease)]
        )
        self.assertIs(result.success, False)
        self.assertEqual(result.code, "requeued_for_preview")
        self.assertEqual(
            result.message,
            "Candidate quality evidence unavailable at import time: stale snapshot; requeued for preview after 120s",
        )

    def test_exception_zero_row_and_exhaustion_have_distinct_exact_outcomes(self) -> None:
        cases = []
        db = _EvidenceDB(
            job=SimpleNamespace(created_at=None),
            requeue_error=RuntimeError("db exploded"),
        )
        cases.append((db, "requeue_failed", "Requeue to preview failed: RuntimeError: db exploded"))
        db = _EvidenceDB(job=SimpleNamespace(created_at=None), requeue_result=None)
        cases.append((db, "requeue_failed", "Candidate quality evidence unavailable at import time: stale snapshot (requeue UPDATE matched zero rows)"))
        for db, code, message in cases:
            with self.subTest(message=message):
                result = _requeue_import_job_to_preview(
                    db, import_job_id=7, reason="stale snapshot"
                )
                self.assertIs(result.success, False)
                self.assertEqual(result.code, code)
                self.assertEqual(result.message, message)

        now = datetime.now(UTC)
        db = _EvidenceDB(
            job=SimpleNamespace(
                created_at=now - timedelta(days=40), attempts=6, preview_attempts=5
            )
        )
        result = _requeue_import_job_to_preview(
            db, import_job_id=7, reason="stale snapshot"
        )
        self.assertIs(result.success, False)
        self.assertEqual(result.code, "requeue_exhausted")
        self.assertIn("preview/import requeue budget exhausted", result.message)
        self.assertIn("attempts=6, preview_attempts=5", result.message)
        self.assertEqual(db.requeue_calls, [])


class TestSidecarBoundary(unittest.TestCase):
    def test_runtime_config_and_rank_policy_reach_sidecar_service(self) -> None:
        cfg = CratediggerConfig(quality_ranks=QualityRankConfig.defaults())
        handle = MagicMock()
        beets = handle.__enter__.return_value
        sentinel = MagicMock()
        import lib.sidecar_service as sidecar_service_module

        validate = MagicMock()
        opened = MagicMock(return_value=handle)
        write = MagicMock(return_value=sentinel)
        with _swap(beets_db_module, "validate_beets_storage_pair", validate), _swap(
            beets_db_module, "open_beets_db", opened
        ), _swap(sidecar_service_module, "write_sidecar_for_request", write):
            result = _write_album_sidecar_after_import(
                MagicMock(), request_id=41, mb_release_id=MBID, cfg=cfg
            )
        self.assertIs(result, sentinel)
        validate.assert_called_once_with(db_path=None, library_root=None)
        opened.assert_called_once_with(cfg)
        write.assert_called_once_with(
            ANY,
            beets,
            41,
            mb_release_id=MBID,
            quality_ranks=cfg.quality_ranks,
        )
