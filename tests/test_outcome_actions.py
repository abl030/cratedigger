"""Outcome-action contract pins for terminal lifecycle and audit writers."""

from __future__ import annotations

import unittest
from dataclasses import replace

import msgspec

from lib.dispatch import (
    _do_mark_done,
    _record_preview_measurement_failed,
    _reject_import_from_evidence_decision,
)
from lib.dispatch.outcome_actions import _record_have_analysis_error
from lib.dispatch.types import (
    DISPATCH_CODE_QUALITY_PIPELINE_REJECTED,
    ImportAttemptResult,
)
from lib.import_evidence import HaveAnalysisFailure
from lib.import_queue import IMPORT_JOB_FORCE
from lib.quality import (
    V0_PROBE_LOSSLESS_SOURCE,
    AudioQualityMeasurement,
    CodecRankBands,
    DownloadInfo,
    ImportResult,
    MeasurementFailure,
    QualityRankConfig,
    SpectralAnalysisDetail,
    SpectralDetail,
    SpectralMeasurement,
    V0ProbeEvidence,
    ValidationResult,
)
from lib.terminal_outcomes import (
    AutomationTerminalAuthority,
    PendingImportTerminalOutcome,
    PreviewTerminalOutcome,
    TerminalDownloadAudit,
    TerminalOutcomeResult,
    cleanup_journal_refusal_disposition,
)
from tests.dispatch_helpers import (
    claim_next_import_job,
    make_dispatch_request,
    patch_dispatch_externals,
)
from tests.fakes import FakePipelineDB
from tests.helpers import make_import_result, make_request_row
from tests.test_pipeline_db import make_db, requires_postgres

_DENYLIST_REASONS = {
    "downgrade": "quality downgrade prevented",
    "suspect_lossless_downgrade": "suspect lossless source not an upgrade",
    "suspect_lossless_probe_missing": "suspect lossless source not an upgrade",
    "lossless_source_locked": "lossless source locked",
    "transcode_downgrade": "rejected: transcode_downgrade",
    "spectral_reject": "spectral analysis rejected the source",
    "audio_corrupt": "audio decode failures",
    "bad_audio_hash": "matched curated bad audio hash",
    "mixed_source": "mixed lossless+lossy source",
    "duplicate_remove_guard_failed": "rejected: duplicate_remove_guard_failed",
}


def _attempt(result: ImportResult, audit: SpectralDetail | None = None) -> ImportAttemptResult:
    attempt = ImportAttemptResult(audit)
    attempt.merge(result)
    return attempt


def _full_download_info() -> DownloadInfo:
    return DownloadInfo(
        username="primary-peer",
        contributor_usernames=("contributor-a", "contributor-b"),
        filetype="flac",
        bitrate=987_000,
        sample_rate=96_000,
        bit_depth=24,
        is_vbr=True,
        was_converted=True,
        original_filetype="wav",
        slskd_filetype="wav",
        actual_filetype="flac",
        actual_min_bitrate=876,
        download_spectral=SpectralMeasurement("genuine", 654),
        current_spectral=SpectralMeasurement("transparent", 543),
        existing_min_bitrate=432,
        verified_lossless_override=True,
        final_format="FLAC",
        v0_probe=V0ProbeEvidence(
            kind=V0_PROBE_LOSSLESS_SOURCE,
            min_bitrate_kbps=201,
            avg_bitrate_kbps=251,
            median_bitrate_kbps=241,
        ),
        existing_v0_probe=V0ProbeEvidence(
            kind=V0_PROBE_LOSSLESS_SOURCE,
            min_bitrate_kbps=181,
            avg_bitrate_kbps=231,
            median_bitrate_kbps=221,
        ),
    )


def _assert_full_audit(test: unittest.TestCase, audit: TerminalDownloadAudit) -> None:
    expected = {
        "soulseek_username": "primary-peer",
        "contributor_usernames": ("contributor-a", "contributor-b"),
        "filetype": "flac",
        "beets_detail": "sentinel detail",
        "outcome": "force_import",
        "staged_path": "/staging/sentinel",
        "bitrate": 987_000,
        "sample_rate": 96_000,
        "bit_depth": 24,
        "is_vbr": True,
        "was_converted": True,
        "original_filetype": "wav",
        "slskd_filetype": "wav",
        "actual_filetype": "flac",
        "actual_min_bitrate": 876,
        "spectral_grade": "genuine",
        "spectral_bitrate": 654,
        "existing_min_bitrate": 432,
        "existing_spectral_bitrate": 543,
        "final_format": "FLAC",
        "v0_probe_kind": V0_PROBE_LOSSLESS_SOURCE,
        "v0_probe_min_bitrate": 201,
        "v0_probe_avg_bitrate": 251,
        "v0_probe_median_bitrate": 241,
        "existing_v0_probe_kind": V0_PROBE_LOSSLESS_SOURCE,
        "existing_v0_probe_min_bitrate": 181,
        "existing_v0_probe_avg_bitrate": 231,
        "existing_v0_probe_median_bitrate": 221,
        "source_download_log_id": 91,
    }
    for name, value in expected.items():
        test.assertEqual(getattr(audit, name), value, name)
    test.assertIsNotNone(audit.import_result)
    test.assertIsNotNone(audit.validation_result)
    assert audit.validation_result is not None
    validation = ValidationResult.from_json(audit.validation_result)
    test.assertTrue(validation.valid)
    test.assertEqual(validation.distance, 0.031)
    test.assertEqual(validation.scenario, "strong_match")
    test.assertEqual(validation.detail, "sentinel detail")


class TestRejectImportOutcomeContracts(unittest.TestCase):
    def _reject(
        self,
        *,
        decision: str,
        job_backed: bool = False,
        dl_info: DownloadInfo | None = None,
        result: ImportResult | None = None,
        quality_ranks: QualityRankConfig | None = None,
        current_override: str | None = "lossless,mp3 320,aac,opus,ogg",
    ):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            status="downloading",
            search_filetype_override=current_override,
        ))
        job_id = None
        if job_backed:
            job = db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                payload={"download_log_id": 91, "failed_path": "/staging/reject"},
            )
            db.mark_import_job_preview_importable(job.id, preview_result={"ready": True})
            claimed = claim_next_import_job(db, worker_id="outcome-contract")
            assert claimed is not None
            job_id = claimed.id
        attempt = _attempt(result or make_import_result(decision=decision))
        db.set_cooldown_result(True)
        cooled_down_users: set[str] = set()
        with patch_dispatch_externals():
            outcome = _reject_import_from_evidence_decision(
                make_dispatch_request(
                    request_id=42,
                    path="/staging/reject",
                    dl_info=(
                        dl_info
                        or DownloadInfo(username="primary-peer", filetype="mp3")
                    ),
                    distance=0.217,
                    scenario="fallback-scenario",
                    requeue_on_failure=not job_backed,
                    candidate_import_job_id=job_id,
                    candidate_download_log_id=91,
                    cooled_down_users=cooled_down_users,
                ),
                db,
                attempt_result=attempt,
                decision=decision,
                detail="sentinel rejection detail",
                quality_ranks=quality_ranks,
                protected_roots=frozenset({"/staging", "/processing/albums"}),
            )
        return db, outcome, cooled_down_users

    def test_fallback_validation_envelope_and_dispatch_signal_are_exact(self) -> None:
        db, outcome, _ = self._reject(decision="nested_layout")

        self.assertIs(outcome.success, False)
        self.assertEqual(outcome.code, DISPATCH_CODE_QUALITY_PIPELINE_REJECTED)
        self.assertEqual(outcome.post_commit_wrong_match_scenario, "nested_layout")
        self.assertEqual(
            db.request(42)["search_filetype_override"],
            "lossless,mp3 320,aac,opus,ogg",
        )
        log = db.download_logs[-1]
        assert isinstance(log.validation_result, str)
        envelope = ValidationResult.from_json(log.validation_result)
        self.assertEqual(envelope.distance, 0.217)
        self.assertEqual(envelope.scenario, "nested_layout")
        self.assertEqual(envelope.detail, "sentinel rejection detail")

    def test_nondefault_rank_policy_controls_both_downgrade_decisions(self) -> None:
        custom = replace(
            QualityRankConfig.defaults(),
            mp3=CodecRankBands(transparent=100, excellent=80, good=60, acceptable=40),
        )
        audit = SpectralDetail(
            candidate=SpectralAnalysisDetail(attempted=True, grade="genuine"),
            existing=SpectralAnalysisDetail(attempted=True, grade="genuine"),
        )
        for decision in ("downgrade", "transcode_downgrade"):
            with self.subTest(decision=decision):
                result = ImportResult(
                    decision=decision,
                    source_measurement=AudioQualityMeasurement(
                        min_bitrate_kbps=90,
                        avg_bitrate_kbps=90,
                        median_bitrate_kbps=90,
                        format="MP3",
                    ),
                    current_measurement=AudioQualityMeasurement(
                        min_bitrate_kbps=120,
                        avg_bitrate_kbps=120,
                        median_bitrate_kbps=120,
                        format="MP3",
                    ),
                    spectral=audit,
                )
                db, _, _ = self._reject(
                    decision=decision,
                    result=result,
                    quality_ranks=custom,
                )
                self.assertEqual(db.request(42)["search_filetype_override"], "lossless")

    def test_transcode_downgrade_preserves_none_blank_and_concrete_overrides(self) -> None:
        result = ImportResult(
            decision="transcode_downgrade",
            source_measurement=AudioQualityMeasurement(
                min_bitrate_kbps=90,
                avg_bitrate_kbps=90,
                median_bitrate_kbps=90,
                format="MP3",
            ),
            current_measurement=AudioQualityMeasurement(
                min_bitrate_kbps=120,
                avg_bitrate_kbps=120,
                median_bitrate_kbps=120,
                format="MP3",
            ),
        )
        for current_override in (None, "", "lossless,mp3 320"):
            with self.subTest(current_override=current_override):
                db, _, _ = self._reject(
                    decision="transcode_downgrade",
                    result=result,
                    current_override=current_override,
                )
                self.assertEqual(
                    db.request(42)["search_filetype_override"],
                    current_override,
                )

    def test_pending_rejection_preserves_source_provenance_and_cleanup_authority(self) -> None:
        _, outcome, _ = self._reject(decision="audio_corrupt", job_backed=True)

        pending = outcome.terminal_outcome
        assert pending is not None
        self.assertEqual(pending.audit.staged_path, "/staging/reject")
        self.assertEqual(pending.audit.source_download_log_id, 91)
        cleanup = outcome.post_commit_cleanup
        assert cleanup is not None
        self.assertEqual(cleanup.staged_path, "/staging/reject")
        self.assertEqual(
            cleanup.staged_path_protected_parents,
            frozenset({"/staging", "/processing/albums"}),
        )

    def test_every_denylist_decision_has_canonical_reason_in_both_lanes(self) -> None:
        for decision, reason in _DENYLIST_REASONS.items():
            with self.subTest(decision=decision, lane="pending"):
                _, outcome, _ = self._reject(decision=decision, job_backed=True)
                pending = outcome.terminal_outcome
                assert pending is not None
                self.assertEqual(
                    pending.denylists,
                    (type(pending.denylists[0])("primary-peer", reason, True),),
                )
            with self.subTest(decision=decision, lane="direct"):
                db, _, cooled_down_users = self._reject(decision=decision)
                self.assertEqual(
                    [(entry.username, entry.reason) for entry in db.denylist],
                    [("primary-peer", reason)],
                )
                self.assertEqual(db.cooldowns_applied, ["primary-peer"])
                self.assertEqual(cooled_down_users, {"primary-peer"})


class TestMarkDoneOutcomeContracts(unittest.TestCase):
    def _call(self, *, job_backed: bool, clear_marked_incomplete: bool = False):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            status="downloading",
            marked_incomplete_at="2026-08-01T00:00:00+00:00",
            current_lossless_source_v0_probe_min_bitrate=111,
            current_lossless_source_v0_probe_avg_bitrate=222,
            current_lossless_source_v0_probe_median_bitrate=212,
        ))
        job_id = None
        if job_backed:
            job = db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                payload={"download_log_id": 91, "failed_path": "/staging/sentinel"},
            )
            db.mark_import_job_preview_importable(job.id, preview_result={"ready": True})
            claimed = claim_next_import_job(db, worker_id="mark-done-contract")
            assert claimed is not None
            job_id = claimed.id
        dl_info = _full_download_info()
        attempt = _attempt(make_import_result(
            decision="import",
            new_min_bitrate=876,
            prev_min_bitrate=432,
            was_converted=True,
            original_filetype="wav",
            target_filetype="flac",
            final_format="FLAC",
        ))
        result = _do_mark_done(
            db,
            42,
            dl_info,
            0.031,
            "strong_match",
            "/staging/sentinel",
            outcome_label="force_import",
            detail="sentinel detail",
            attempt_result=attempt,
            import_job_id=job_id,
            source_download_log_id=91,
            clear_marked_incomplete=clear_marked_incomplete,
        )
        return db, result

    def test_fully_populated_direct_audit_preserves_every_field(self) -> None:
        db, result = self._call(job_backed=False)

        self.assertIsInstance(result, int)
        row = db.request(42)
        self.assertEqual(row["status"], "imported")
        self.assertTrue(row["verified_lossless"])
        self.assertEqual(row["final_format"], "FLAC")
        self.assertEqual(row["current_spectral_grade"], "genuine")
        self.assertEqual(row["current_spectral_bitrate"], 987)
        self.assertEqual(
            row["current_lossless_source_v0_probe_min_bitrate"],
            201,
        )
        self.assertEqual(
            row["current_lossless_source_v0_probe_avg_bitrate"],
            251,
        )
        log = db.download_logs[-1]
        audit = TerminalDownloadAudit(
            outcome="force_import",
            soulseek_username=log.soulseek_username,
            contributor_usernames=tuple(log.candidate_contributor_usernames or ()),
            filetype=log.filetype,
            beets_distance=log.beets_distance,
            beets_scenario=log.beets_scenario,
            beets_detail=log.beets_detail,
            staged_path=log.staged_path,
            import_result=log.import_result,
            validation_result=log.validation_result,
            source_download_log_id=log.source_download_log_id,
            **log.extra,
        )
        _assert_full_audit(self, audit)

    def test_fully_populated_pending_audit_preserves_every_field(self) -> None:
        _, result = self._call(job_backed=True)

        self.assertIsInstance(result, PendingImportTerminalOutcome)
        assert isinstance(result, PendingImportTerminalOutcome)
        _assert_full_audit(self, result.audit)

    def test_acceptance_defaults_clear_stale_probe_but_not_incomplete_mark(self) -> None:
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            status="downloading",
            marked_incomplete_at="2026-08-01T00:00:00+00:00",
            current_lossless_source_v0_probe_min_bitrate=111,
            current_lossless_source_v0_probe_avg_bitrate=222,
            current_lossless_source_v0_probe_median_bitrate=212,
        ))
        _do_mark_done(
            db,
            42,
            DownloadInfo(final_format="MP3"),
            0.1,
            "strong_match",
            "/staging/defaults",
        )
        row = db.request(42)
        self.assertIsNone(row["current_lossless_source_v0_probe_min_bitrate"])
        self.assertIsNone(row["current_lossless_source_v0_probe_avg_bitrate"])
        self.assertIsNone(row["current_lossless_source_v0_probe_median_bitrate"])
        self.assertEqual(row["marked_incomplete_at"], "2026-08-01T00:00:00+00:00")

    def test_whole_candidate_clear_is_atomic_with_acceptance(self) -> None:
        db, _ = self._call(job_backed=False, clear_marked_incomplete=True)
        self.assertIsNone(db.request(42)["marked_incomplete_at"])


class _PreviewCaptureDB(FakePipelineDB):
    def persist_preview_terminal_outcome(
        self,
        command: PreviewTerminalOutcome,
    ) -> TerminalOutcomeResult:
        self.persist_preview_terminal_outcome_calls.append(command)
        job = self.get_import_job(command.import_job_id)
        assert job is not None
        return TerminalOutcomeResult(
            download_log_id=733,
            job=job,
            transitions=(),
        )


class TestPreviewFailureOutcomeContracts(unittest.TestCase):
    def test_automation_and_force_commands_preserve_failure_evidence(self) -> None:
        failure = MeasurementFailure(
            reason="snapshot_stale",
            detail="snapshot changed during measurement",
            source_path="/staging/preview",
        )
        import_result = make_import_result(
            decision="measurement_failed",
            error="ffprobe failed",
        )
        authority = AutomationTerminalAuthority(
            expected_job_status="queued",
            expected_preview_status="running",
            expected_execution_lease=None,
            cleanup_receipt=None,
            cleanup_refusal=cleanup_journal_refusal_disposition(
                None,
                error_code="preview_failed",
                error_message="preview failed before cleanup",
            ),
        )
        for label, requeue, automation in (
            ("automation", True, authority),
            ("force", False, None),
        ):
            with self.subTest(label=label):
                db = _PreviewCaptureDB()
                job = db.enqueue_import_job(
                    IMPORT_JOB_FORCE,
                    request_id=42,
                    payload={
                        "download_log_id": 91,
                        "failed_path": "/staging/preview",
                    },
                )
                result_id = _record_preview_measurement_failed(
                    db,
                    request_id=42,
                    import_job_id=job.id,
                    payload=failure,
                    import_result=import_result,
                    preview_result={"producer": label},
                    requeue_to_wanted=requeue,
                    automation_terminal_authority=automation,
                )
                self.assertEqual(result_id, 733)
                command = db.persist_preview_terminal_outcome_calls[-1]
                transition = command.request_transition
                if label == "automation":
                    assert transition is not None
                    self.assertEqual(transition.from_status, "processing")
                    self.assertEqual(transition.target_status, "wanted")
                else:
                    self.assertIsNone(transition)
                self.assertEqual(command.audit.error_message, failure.detail)
                self.assertEqual(command.audit.import_result, import_result.to_json())
                self.assertEqual(command.audit.staged_path, failure.source_path)
                self.assertEqual(command.message, failure.detail)
                self.assertEqual(command.error, failure.reason)
                self.assertEqual(command.preview_result, {"producer": label})
                self.assertEqual(
                    msgspec.json.decode(command.audit.validation_result or "{}"),
                    msgspec.to_builtins(failure),
                )


class TestHaveAnalysisFailureContracts(unittest.TestCase):
    def _call(
        self,
        *,
        job_backed: bool,
        requeue: bool,
        raw_error: str,
        snapshot_guard: str | None,
        cooldown_verdict: bool = True,
    ):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="downloading"))
        job_id = None
        if job_backed:
            job = db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=42,
                payload={"download_log_id": 91, "failed_path": "/staging/candidate"},
            )
            db.mark_import_job_preview_importable(job.id, preview_result={"ready": True})
            claimed = claim_next_import_job(db, worker_id="have-contract")
            assert claimed is not None
            job_id = claimed.id
        cooled: set[str] = set()
        db.set_cooldown_result(cooldown_verdict)
        result = _record_have_analysis_error(
            make_dispatch_request(
                request_id=42,
                path="/staging/candidate",
                dl_info=DownloadInfo(
                    username="analysis-peer",
                    contributor_usernames=("contributor-peer",),
                    filetype="flac",
                ),
                requeue_on_failure=requeue,
                candidate_import_job_id=job_id,
                candidate_download_log_id=91,
                cooled_down_users=cooled,
            ),
            db,
            raw_error=raw_error,
            installed_path="/library/installed",
            snapshot_guard=snapshot_guard,
        )
        return db, cooled, result

    def test_failure_taxonomy_and_audit_are_preserved_in_both_lanes(self) -> None:
        cases = (
            ("installed evidence changed", "stale", "snapshot_changed"),
            ("missing path", None, "path_missing"),
            ("PermissionError: denied", "matched", "permission_denied"),
            ("ffprobe analyser crashed", "matched", "analyser_failure"),
        )
        for job_backed in (False, True):
            for raw_error, snapshot_guard, expected_category in cases:
                with self.subTest(job_backed=job_backed, raw_error=raw_error):
                    db, _, result = self._call(
                        job_backed=job_backed,
                        requeue=True,
                        raw_error=raw_error,
                        snapshot_guard=snapshot_guard,
                    )
                    audit = result.audit if isinstance(result, PendingImportTerminalOutcome) else None
                    if audit is None:
                        log = db.download_logs[-1]
                        audit = TerminalDownloadAudit(
                            outcome="have_analysis_error",
                            soulseek_username=log.soulseek_username,
                            contributor_usernames=tuple(log.candidate_contributor_usernames or ()),
                            filetype=log.filetype,
                            download_path=log.extra["download_path"],
                            beets_scenario=log.beets_scenario,
                            beets_detail=log.beets_detail,
                            staged_path=log.staged_path,
                            error_message=log.error_message,
                            validation_result=log.validation_result,
                            source_download_log_id=log.source_download_log_id,
                        )
                    payload = msgspec.json.decode(
                        audit.validation_result or "{}",
                        type=HaveAnalysisFailure,
                    )
                    self.assertEqual(payload.error, raw_error)
                    self.assertEqual(payload.failure_category, expected_category)
                    self.assertEqual(payload.installed_path, "/library/installed")
                    self.assertEqual(payload.candidate_reference, "/staging/candidate")
                    self.assertEqual(audit.download_path, "/library/installed")
                    self.assertEqual(
                        audit.contributor_usernames,
                        ("contributor-peer",),
                    )
                    self.assertEqual(audit.filetype, "flac")
                    self.assertEqual(audit.staged_path, "/staging/candidate")
                    self.assertEqual(audit.error_message, raw_error)
                    self.assertEqual(audit.source_download_log_id, 91)
                    self.assertEqual(
                        audit.beets_detail,
                        f"Installed HAVE analysis failed ({payload.failure_category}): {raw_error}",
                    )
                    if isinstance(result, PendingImportTerminalOutcome):
                        assert result.initial_transition is not None
                        self.assertEqual(result.initial_transition.attempt_type, "validation")
                        self.assertEqual(result.cooldowns[0].username, "analysis-peer")

    def test_jobless_requeue_and_retained_worlds_terminalize_and_cool_down(self) -> None:
        for requeue in (False, True):
            with self.subTest(requeue=requeue):
                db, cooled, result = self._call(
                    job_backed=False,
                    requeue=requeue,
                    raw_error="PermissionError: denied",
                    snapshot_guard="matched",
                )
                self.assertIsInstance(result, int)
                self.assertEqual(db.request(42)["status"], "wanted" if requeue else "downloading")
                self.assertEqual(db.request(42)["validation_attempts"], 1 if requeue else 0)
                self.assertEqual(db.cooldowns_applied, ["analysis-peer"])
                self.assertEqual(cooled, {"analysis-peer"})
                self.assertEqual(len(db.download_logs), 1)

    def test_jobless_false_cooldown_verdict_does_not_pollute_cycle_roster(self) -> None:
        db, cooled, _ = self._call(
            job_backed=False,
            requeue=True,
            raw_error="PermissionError: denied",
            snapshot_guard="matched",
            cooldown_verdict=False,
        )
        self.assertEqual(db.cooldowns_applied, ["analysis-peer"])
        self.assertEqual(cooled, set())


@requires_postgres
class TestOutcomeActionsPostgresRoundTrip(unittest.TestCase):
    def setUp(self) -> None:
        self.db = make_db()

    def tearDown(self) -> None:
        self.db.close()

    def test_acceptance_and_rejection_audits_round_trip_exact_fields(self) -> None:
        accepted_id = self.db.add_request(
            artist_name="Outcome Contract Artist",
            album_title="Accepted Album",
            source="request",
            mb_release_id="outcome-contract-accepted",
            status="downloading",
        )
        accepted_info = _full_download_info()
        accepted_attempt = _attempt(make_import_result(
            decision="import",
            new_min_bitrate=876,
            prev_min_bitrate=432,
            was_converted=True,
            original_filetype="wav",
            target_filetype="flac",
            final_format="FLAC",
        ))
        source_log_id = self.db.log_download(
            accepted_id,
            outcome="rejected",
            staged_path="/staging/original",
        )
        _do_mark_done(
            self.db,
            accepted_id,
            accepted_info,
            0.031,
            "strong_match",
            "/staging/sentinel",
            outcome_label="force_import",
            detail="sentinel detail",
            attempt_result=accepted_attempt,
            source_download_log_id=source_log_id,
        )
        accepted_history = self.db.get_download_history(accepted_id)
        accepted = accepted_history[0]
        self.assertEqual(accepted["outcome"], "force_import")
        self.assertEqual(accepted["soulseek_username"], "primary-peer")
        self.assertEqual(
            tuple(accepted["candidate_contributor_usernames"] or ()),
            ("contributor-a", "contributor-b"),
        )
        for name, value in {
            "filetype": "flac",
            "beets_detail": "sentinel detail",
            "staged_path": "/staging/sentinel",
            "bitrate": 987_000,
            "sample_rate": 96_000,
            "bit_depth": 24,
            "is_vbr": True,
            "was_converted": True,
            "original_filetype": "wav",
            "slskd_filetype": "wav",
            "actual_filetype": "flac",
            "actual_min_bitrate": 876,
            "spectral_grade": "genuine",
            "spectral_bitrate": 654,
            "existing_min_bitrate": 432,
            "existing_spectral_bitrate": 543,
            "final_format": "FLAC",
            "v0_probe_kind": V0_PROBE_LOSSLESS_SOURCE,
            "v0_probe_min_bitrate": 201,
            "v0_probe_avg_bitrate": 251,
            "v0_probe_median_bitrate": 241,
            "existing_v0_probe_kind": V0_PROBE_LOSSLESS_SOURCE,
            "existing_v0_probe_min_bitrate": 181,
            "existing_v0_probe_avg_bitrate": 231,
            "existing_v0_probe_median_bitrate": 221,
            "source_download_log_id": source_log_id,
        }.items():
            self.assertEqual(accepted[name], value, name)
        self.assertIsInstance(accepted["import_result"], dict)
        self.assertIsInstance(accepted["validation_result"], dict)

        rejected_id = self.db.add_request(
            artist_name="Outcome Contract Artist",
            album_title="Rejected Album",
            source="request",
            mb_release_id="outcome-contract-rejected",
            status="downloading",
        )
        rejected_source_log_id = self.db.log_download(
            rejected_id,
            outcome="rejected",
            staged_path="/staging/rejected-original",
        )
        attempt = _attempt(make_import_result(decision="nested_layout"))
        with patch_dispatch_externals():
            _reject_import_from_evidence_decision(
                make_dispatch_request(
                    request_id=rejected_id,
                    path="/staging/rejected",
                    dl_info=DownloadInfo(username="rejected-peer"),
                    distance=0.417,
                    scenario="nested_layout",
                    requeue_on_failure=True,
                    candidate_download_log_id=rejected_source_log_id,
                ),
                self.db,
                attempt_result=attempt,
                decision="nested_layout",
                detail="nested candidate layout",
            )
        rejected = self.db.get_download_history(rejected_id)[0]
        self.assertEqual(rejected["staged_path"], "/staging/rejected")
        self.assertEqual(
            rejected["source_download_log_id"],
            rejected_source_log_id,
        )
        self.assertEqual(rejected["beets_distance"], 0.417)
        self.assertEqual(rejected["beets_scenario"], "nested_layout")
        rejection_validation = rejected["validation_result"]
        assert rejection_validation is not None
        self.assertEqual(
            rejection_validation["detail"],
            "nested candidate layout",
        )


if __name__ == "__main__":
    unittest.main()
