"""Durable runtime and static contracts for ``lib.dispatch.types`` (#1321).

The module is mostly decorated dataclasses, Protocols, aliases, and module-level
constants: real contracts that mutmut 3.7 does not meaningfully catalogue. These
pins are the deterministic boundary for the separately recorded 32-fault aimed
matrix; generated dispatch properties remain complementary patrols, not catalog
selectors.
"""

from __future__ import annotations

import dataclasses
import inspect
import json
import typing
import unittest
from unittest import mock

from lib import dispatch
from lib.dispatch.types import (
    _PREIMPORT_FACT_REJECT_DECISIONS,
    DISPATCH_CODE_BAD_REQUEST,
    DISPATCH_CODE_IMPORT_MANIFEST_REJECTED,
    DISPATCH_CODE_PROCESSING_LOCKED,
    DISPATCH_CODE_QUALITY_PIPELINE_REJECTED,
    DISPATCH_CODE_REQUEUE_EXHAUSTED,
    DISPATCH_CODE_REQUEUE_FAILED,
    DISPATCH_CODE_REQUEUED_FOR_PREVIEW,
    FORCE_IMPORT_SCENARIOS,
    DispatchDB,
    DispatchOutcome,
    DispatchRequest,
    EvidenceImportGate,
    ImportAttemptResult,
    ImportOneRun,
    PostCommitCleanup,
    QualityGateState,
)
from lib.quality import (
    AudioQualityMeasurement,
    DownloadInfo,
    ImportResult,
    SpectralAnalysisDetail,
    SpectralCodecContext,
    SpectralDetail,
)
from lib.terminal_outcomes import PendingImportTerminalOutcome, TerminalDownloadAudit
from lib.wrong_match_policy import PREIMPORT_FACT_REJECTION_SCENARIOS
from tests.fakes import FakePipelineDB


class TestDispatchTypeContracts(unittest.TestCase):
    def test_durable_dispatch_codes_are_exact_literals(self) -> None:
        self.assertEqual(
            (
                DISPATCH_CODE_REQUEUED_FOR_PREVIEW,
                DISPATCH_CODE_REQUEUE_FAILED,
                DISPATCH_CODE_REQUEUE_EXHAUSTED,
                DISPATCH_CODE_BAD_REQUEST,
                DISPATCH_CODE_PROCESSING_LOCKED,
                DISPATCH_CODE_QUALITY_PIPELINE_REJECTED,
                DISPATCH_CODE_IMPORT_MANIFEST_REJECTED,
            ),
            (
                "requeued_for_preview",
                "requeue_failed",
                "requeue_exhausted",
                "bad_request",
                "processing_locked",
                "quality_pipeline_rejected",
                "import_manifest_rejected",
            ),
        )

    def test_force_import_scenarios_exact_literal_membership(self) -> None:
        self.assertEqual(FORCE_IMPORT_SCENARIOS, frozenset({"force_import"}))

    def test_preimport_fact_reject_decisions_is_taxonomy_alias(self) -> None:
        self.assertIs(
            _PREIMPORT_FACT_REJECT_DECISIONS,
            PREIMPORT_FACT_REJECTION_SCENARIOS,
        )

    def test_dispatch_request_policy_defaults_are_exact(self) -> None:
        specs = {field.name: field.default for field in dataclasses.fields(DispatchRequest)}
        self.assertIs(specs["force"], False)
        self.assertEqual(specs["scenario"], "auto_import")
        self.assertEqual(specs["outcome_label"], "success")
        self.assertIs(specs["requeue_on_failure"], True)

    def test_dispatch_db_is_runtime_checkable(self) -> None:
        # Several inherited DB ports are runtime-checkable too, so an
        # isinstance-only assertion cannot prove this public composite keeps
        # its own explicit runtime contract.
        self.assertTrue(inspect.getsource(DispatchDB).startswith("@runtime_checkable\n"))
        self.assertIsInstance(FakePipelineDB(), DispatchDB)

    def test_dispatch_db_member_set_is_exact(self) -> None:
        self.assertEqual(
            typing.get_protocol_members(DispatchDB),
            frozenset({
                "_probe_owner_session",
                "add_jellyfin_date_created_pin",
                "add_plex_added_at_pin",
                "advisory_lock",
                "authorize_import_job_launch",
                "capture_automation_import_completion",
                "compare_request_status",
                "find_album_quality_evidence",
                "get_download_log_candidate_evidence_id",
                "get_download_log_entry",
                "get_import_job",
                "get_import_job_candidate_evidence_id",
                "get_oldest_request_chain_created_at",
                "get_pending_jellyfin_date_created_pins",
                "get_pending_plex_added_at_pins",
                "get_recent_successful_uploader",
                "get_request",
                "get_request_current_evidence_id",
                "get_tracks",
                "heartbeat_import_job",
                "heartbeat_import_job_preview",
                "load_album_quality_evidence_by_id",
                "log_download",
                "mark_imported_with_rescue",
                "mark_jellyfin_date_created_pin",
                "mark_plex_added_at_pin",
                "merge_rekey_collision",
                "persist_import_terminal_outcome",
                "persist_preview_terminal_outcome",
                "persist_request_policy_outcome",
                "persist_request_rejection_outcome",
                "persist_request_success_outcome",
                "record_attempt",
                "record_import_job_beets_child",
                "request_marked_incomplete",
                "requeue_import_job_for_preview",
                "reset_downloading_to_wanted",
                "reset_to_wanted",
                "set_download_log_candidate_evidence",
                "set_downloading",
                "set_import_job_candidate_evidence",
                "set_request_current_evidence",
                "update_request_release_for_merge",
                "update_status",
                "upsert_album_quality_evidence",
            }),
        )

    def test_dispatch_package_exports_are_exact(self) -> None:
        self.assertEqual(
            dispatch.__all__,
            [
                "DISPATCH_CODE_BAD_REQUEST",
                "DISPATCH_CODE_IMPORT_MANIFEST_REJECTED",
                "DISPATCH_CODE_PROCESSING_LOCKED",
                "DISPATCH_CODE_QUALITY_PIPELINE_REJECTED",
                "DISPATCH_CODE_REQUEUED_FOR_PREVIEW",
                "DISPATCH_CODE_REQUEUE_EXHAUSTED",
                "DISPATCH_CODE_REQUEUE_FAILED",
                "DispatchCoreFn",
                "DispatchDB",
                "DispatchOutcome",
                "DispatchRequest",
                "QualityGateFn",
                "_build_download_info",
                "_check_quality_gate_core",
                "_cleanup_staged_dir",
                "_do_mark_done",
                "_download_info_from_candidate_evidence",
                "_load_evidence_import_gate",
                "_populate_dl_info_from_import_result",
                "_record_preview_measurement_failed",
                "_record_rejection_and_maybe_requeue",
                "_refresh_current_evidence_after_import",
                "_reject_import_from_evidence_decision",
                "_requeue_import_job_to_preview",
                "_write_album_sidecar_after_import",
                "build_import_one_command",
                "dispatch_import_core",
                "dispatch_import_from_db",
                "load_quality_gate_state",
                "run_import_one",
            ],
        )


class TestImportAttemptResult(unittest.TestCase):
    @staticmethod
    def _audit() -> SpectralDetail:
        return SpectralDetail(
            candidate=SpectralAnalysisDetail(
                attempted=True,
                grade="genuine",
                bitrate_kbps=192,
            ),
        )

    def test_explicit_audit_short_circuits_job_lookup(self) -> None:
        db = FakePipelineDB()
        audit = self._audit()
        with mock.patch.object(db, "get_import_job") as get_import_job:
            attempt = ImportAttemptResult.from_import_job(db, 91, audit)
        get_import_job.assert_not_called()
        self.assertIs(attempt.audit, audit)

    def test_missing_import_job_yields_empty_audit(self) -> None:
        db = FakePipelineDB()
        with (
            mock.patch.object(db, "get_import_job", return_value=None),
            self.assertNoLogs("cratedigger", level="WARNING"),
        ):
            attempt = ImportAttemptResult.from_import_job(db, 91)
        self.assertIsNone(attempt.audit)

    def test_malformed_preview_is_contained_as_empty_audit(self) -> None:
        db = FakePipelineDB()
        job = mock.MagicMock(
            preview_result={"import_result": {"version": 4, "spectral": "bad"}},
        )
        with (
            mock.patch.object(db, "get_import_job", return_value=job),
            self.assertLogs("cratedigger", level="WARNING") as logged,
        ):
            attempt = ImportAttemptResult.from_import_job(db, 91)
        self.assertIsNone(attempt.audit)
        self.assertEqual(len(logged.records), 1)
        self.assertIn("Unable to decode preview spectral audit", logged.output[0])

    def test_from_import_job_recovers_decoded_preview_spectral_audit(self) -> None:
        db = FakePipelineDB()
        audit = self._audit()
        raw: dict[str, object] = json.loads(ImportResult(spectral=audit).to_json())
        job = mock.MagicMock(preview_result={"import_result": raw})
        with mock.patch.object(db, "get_import_job", return_value=job):
            attempt = ImportAttemptResult.from_import_job(db, 91)
        self.assertEqual(attempt.audit, audit)

    def test_merge_preserves_rich_result_and_exact_preview_audit(self) -> None:
        audit = self._audit()
        result = ImportResult(decision="import", final_format="flac")
        attempt = ImportAttemptResult(audit)
        self.assertIs(attempt.merge(result), result)
        self.assertIs(attempt.result, result)
        self.assertIs(result.spectral, audit)
        self.assertEqual(result.decision, "import")
        self.assertEqual(result.final_format, "flac")

    def test_merge_retains_result_identity(self) -> None:
        result = ImportResult(decision="import")
        attempt = ImportAttemptResult(None)
        attempt.merge(result)
        self.assertIs(attempt.result, result)

    def test_apply_before_merge_is_refused(self) -> None:
        attempt = ImportAttemptResult(None)
        with self.assertRaisesRegex(RuntimeError, "before a result exists"):
            attempt.apply(lambda result: setattr(result, "final_format", "flac"))

    def test_finalize_synthesizes_audit_only_result(self) -> None:
        audit = self._audit()
        attempt = ImportAttemptResult(audit)
        dl_info = DownloadInfo()
        attempt.finalize_into(dl_info)
        self.assertIsNotNone(attempt.result)
        self.assertIsNotNone(dl_info.import_result)
        assert dl_info.import_result is not None
        self.assertEqual(ImportResult.from_json(dl_info.import_result).spectral, audit)

    def test_apply_mutation_is_included_only_when_owner_finalizes(self) -> None:
        result = ImportResult(decision="import")
        attempt = ImportAttemptResult(None)
        attempt.merge(result)
        attempt.apply(lambda owned: setattr(owned, "final_format", "flac"))
        dl_info = DownloadInfo()
        attempt.finalize_into(dl_info)
        self.assertIsNotNone(dl_info.import_result)
        assert dl_info.import_result is not None
        self.assertEqual(ImportResult.from_json(dl_info.import_result).final_format, "flac")


class TestDispatchValueCarrierContracts(unittest.TestCase):
    def test_quality_gate_state_retains_spectral_context(self) -> None:
        measurement = AudioQualityMeasurement(format="MP3", min_bitrate_kbps=192)
        context = SpectralCodecContext(
            codec_family="mp3",
            storage_format="mp3",
            filetype_band="mp3",
        )
        state = QualityGateState(
            measurement=measurement,
            verified_lossless_proof=True,
            source_v0_avg_bitrate_kbps=247,
            spectral_context=context,
        )
        self.assertIs(state.measurement, measurement)
        self.assertIs(state.spectral_context, context)
        self.assertIs(state.verified_lossless_proof, True)
        self.assertEqual(state.source_v0_avg_bitrate_kbps, 247)

    def test_post_commit_cleanup_retains_all_fields(self) -> None:
        parents = frozenset({"/processing/albums", "/auto-import"})
        cleanup = PostCommitCleanup(
            staged_path="/processing/albums/candidate",
            staged_path_protected_parents=parents,
            duplicate_guard_source_path="/source",
            duplicate_guard_staging_dir="/staging",
            duplicate_guard_request_id=73,
        )
        self.assertEqual(cleanup.staged_path, "/processing/albums/candidate")
        self.assertIs(cleanup.staged_path_protected_parents, parents)
        self.assertEqual(cleanup.duplicate_guard_source_path, "/source")
        self.assertEqual(cleanup.duplicate_guard_staging_dir, "/staging")
        self.assertEqual(cleanup.duplicate_guard_request_id, 73)

    def test_dispatch_outcome_retains_code_and_terminal_plan(self) -> None:
        terminal = PendingImportTerminalOutcome(
            request_id=73,
            import_job_id=91,
            initial_transition=None,
            audit=TerminalDownloadAudit(outcome="rejected"),
        )
        outcome = DispatchOutcome(
            success=False,
            message="rejected",
            deferred=True,
            code="quality_pipeline_rejected",
            terminal_outcome=terminal,
            post_commit_wrong_match_scenario="strong_match",
        )
        self.assertIs(outcome.success, False)
        self.assertEqual(outcome.message, "rejected")
        self.assertIs(outcome.deferred, True)
        self.assertEqual(outcome.code, "quality_pipeline_rejected")
        self.assertIs(outcome.terminal_outcome, terminal)
        self.assertEqual(outcome.post_commit_wrong_match_scenario, "strong_match")

    def test_import_one_run_retains_subprocess_fields(self) -> None:
        result = ImportResult(decision="import")
        run = ImportOneRun(
            command=("python", "import_one.py"),
            returncode=17,
            stdout="stdout-sentinel",
            stderr="stderr-sentinel",
            import_result=result,
        )
        self.assertEqual(run.command, ("python", "import_one.py"))
        self.assertEqual(run.returncode, 17)
        self.assertEqual(run.stdout, "stdout-sentinel")
        self.assertEqual(run.stderr, "stderr-sentinel")
        self.assertIs(run.import_result, result)

    def test_evidence_import_gate_retains_status_reason_path_and_snapshot_guards(self) -> None:
        gate = EvidenceImportGate(
            candidate_status="candidate-ready",
            candidate_reason="candidate-reason",
            current_status="current-ready",
            current_reason="current-reason",
            current_path="/library/album",
            current_snapshot_guard="current-guard",
            snapshot_guard="candidate-guard",
        )
        self.assertEqual(gate.candidate_status, "candidate-ready")
        self.assertEqual(gate.candidate_reason, "candidate-reason")
        self.assertEqual(gate.current_status, "current-ready")
        self.assertEqual(gate.current_reason, "current-reason")
        self.assertEqual(gate.current_path, "/library/album")
        self.assertEqual(gate.current_snapshot_guard, "current-guard")
        self.assertEqual(gate.snapshot_guard, "candidate-guard")


if __name__ == "__main__":
    unittest.main()
