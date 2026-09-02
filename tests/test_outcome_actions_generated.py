"""Generated world-space patrols for dispatch outcome writers."""

from __future__ import annotations

import unittest

import msgspec
from hypothesis import given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401 - registers active profile
from lib.dispatch import _do_mark_done, _reject_import_from_evidence_decision
from lib.dispatch.outcome_actions import _record_have_analysis_error
from lib.import_evidence import HaveAnalysisFailure
from lib.import_queue import IMPORT_JOB_FORCE
from lib.quality import DownloadInfo, ValidationResult
from lib.terminal_outcomes import PendingImportTerminalOutcome
from tests.dispatch_helpers import (
    claim_next_import_job,
    make_dispatch_request,
    patch_dispatch_externals,
)
from tests.fakes import FakePipelineDB
from tests.helpers import make_import_result, make_request_row
from tests.test_outcome_actions import _DENYLIST_REASONS, _attempt

_OPTIONAL_TEXT = st.one_of(st.none(), st.sampled_from(("", "mp3", "flac")))
_OPTIONAL_INT = st.one_of(st.none(), st.integers(min_value=0, max_value=1_500_000))


class TestGeneratedRejectionOutcomeContracts(unittest.TestCase):
    @given(
        decision=st.sampled_from(tuple(_DENYLIST_REASONS)),
        job_backed=st.booleans(),
        distance=st.one_of(
            st.none(),
            st.floats(
                min_value=0.0,
                max_value=1.0,
                allow_nan=False,
                allow_infinity=False,
            ),
        ),
        source_log_id=st.integers(min_value=1, max_value=10_000),
        current_override=st.one_of(
            st.none(),
            st.sampled_from(("", "lossless", "lossless,mp3 320")),
        ),
        cooldown_verdict=st.booleans(),
    )
    def test_rejection_envelope_provenance_reason_and_cooldown_survive(
        self,
        decision: str,
        job_backed: bool,
        distance: float | None,
        source_log_id: int,
        current_override: str | None,
        cooldown_verdict: bool,
    ) -> None:
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
                payload={"download_log_id": source_log_id, "failed_path": "/generated"},
            )
            db.mark_import_job_preview_importable(job.id, preview_result={"ready": True})
            claimed = claim_next_import_job(db, worker_id="generated-outcome")
            assert claimed is not None
            job_id = claimed.id
        cooled: set[str] = set()
        db.set_cooldown_result(cooldown_verdict)
        detail = f"generated detail for {decision}"
        with patch_dispatch_externals():
            outcome = _reject_import_from_evidence_decision(
                make_dispatch_request(
                    request_id=42,
                    path="/generated/staged",
                    dl_info=DownloadInfo(username="generated-peer"),
                    distance=distance,
                    scenario="generated-fallback",
                    requeue_on_failure=not job_backed,
                    candidate_import_job_id=job_id,
                    candidate_download_log_id=source_log_id,
                    cooled_down_users=cooled,
                ),
                db,
                attempt_result=_attempt(make_import_result(decision=decision)),
                decision=decision,
                detail=detail,
            )

        if job_backed:
            pending = outcome.terminal_outcome
            assert pending is not None
            audit = pending.audit
            self.assertEqual(
                [(item.username, item.reason, item.apply_cooldown) for item in pending.denylists],
                [("generated-peer", _DENYLIST_REASONS[decision], True)],
            )
        else:
            audit_row = db.download_logs[-1]
            audit = None
            self.assertEqual(
                [(item.username, item.reason) for item in db.denylist],
                [("generated-peer", _DENYLIST_REASONS[decision])],
            )
            self.assertEqual(db.cooldowns_applied, ["generated-peer"])
            self.assertEqual(
                cooled,
                {"generated-peer"} if cooldown_verdict else set(),
            )
            self.assertEqual(audit_row.staged_path, "/generated/staged")
            self.assertEqual(audit_row.source_download_log_id, source_log_id)
        if audit is not None:
            self.assertEqual(audit.staged_path, "/generated/staged")
            self.assertEqual(audit.source_download_log_id, source_log_id)
            validation_raw = audit.validation_result
        else:
            validation_raw = db.download_logs[-1].validation_result
        assert validation_raw is not None
        validation = ValidationResult.from_json(validation_raw)
        self.assertEqual(validation.distance, distance)
        self.assertEqual(validation.scenario, decision)
        self.assertEqual(validation.detail, detail)
        self.assertIs(outcome.success, False)
        self.assertEqual(outcome.code, "quality_pipeline_rejected")


class TestGeneratedAcceptedOutcomeContracts(unittest.TestCase):
    @given(
        job_backed=st.booleans(),
        username=_OPTIONAL_TEXT,
        filetype=_OPTIONAL_TEXT,
        bitrate=_OPTIONAL_INT,
        sample_rate=_OPTIONAL_INT,
        bit_depth=st.one_of(st.none(), st.integers(min_value=1, max_value=64)),
        is_vbr=st.one_of(st.none(), st.booleans()),
        final_format=_OPTIONAL_TEXT,
        source_log_id=st.one_of(st.none(), st.integers(min_value=1, max_value=10_000)),
        clear_marked_incomplete=st.booleans(),
        clear_stale_v0_probe=st.booleans(),
    )
    def test_optional_audit_and_acceptance_state_round_trip(
        self,
        job_backed: bool,
        username: str | None,
        filetype: str | None,
        bitrate: int | None,
        sample_rate: int | None,
        bit_depth: int | None,
        is_vbr: bool | None,
        final_format: str | None,
        source_log_id: int | None,
        clear_marked_incomplete: bool,
        clear_stale_v0_probe: bool,
    ) -> None:
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
                payload={"download_log_id": 1, "failed_path": "/generated/accepted"},
            )
            db.mark_import_job_preview_importable(job.id, preview_result={"ready": True})
            claimed = claim_next_import_job(db, worker_id="generated-accepted")
            assert claimed is not None
            job_id = claimed.id
        result = _do_mark_done(
            db,
            42,
            DownloadInfo(
                username=username,
                contributor_usernames=("generated-contributor",),
                filetype=filetype,
                bitrate=bitrate,
                sample_rate=sample_rate,
                bit_depth=bit_depth,
                is_vbr=is_vbr,
                final_format=final_format,
            ),
            0.125,
            "generated_accept",
            "/generated/accepted",
            import_job_id=job_id,
            source_download_log_id=source_log_id,
            clear_marked_incomplete=clear_marked_incomplete,
            clear_stale_v0_probe=clear_stale_v0_probe,
        )
        if isinstance(result, PendingImportTerminalOutcome):
            audit = result.audit
        else:
            row = db.download_logs[-1]
            audit = None
            self.assertEqual(row.soulseek_username, username)
            self.assertEqual(row.filetype, filetype)
            self.assertEqual(row.extra["bitrate"], bitrate)
            self.assertEqual(row.extra["sample_rate"], sample_rate)
            self.assertEqual(row.extra["bit_depth"], bit_depth)
            self.assertEqual(row.extra["is_vbr"], is_vbr)
            self.assertEqual(row.extra["final_format"], final_format)
            self.assertEqual(row.source_download_log_id, source_log_id)
        if audit is not None:
            self.assertEqual(audit.soulseek_username, username)
            self.assertEqual(audit.contributor_usernames, ("generated-contributor",))
            self.assertEqual(audit.filetype, filetype)
            self.assertEqual(audit.bitrate, bitrate)
            self.assertEqual(audit.sample_rate, sample_rate)
            self.assertEqual(audit.bit_depth, bit_depth)
            self.assertEqual(audit.is_vbr, is_vbr)
            self.assertEqual(audit.final_format, final_format)
            self.assertEqual(audit.source_download_log_id, source_log_id)
        request = db.request(42)
        if not job_backed:
            self.assertEqual(request["status"], "imported")
            self.assertEqual(
                request["marked_incomplete_at"],
                None if clear_marked_incomplete else "2026-08-01T00:00:00+00:00",
            )
            expected_probe = None if clear_stale_v0_probe else 111
            self.assertEqual(
                request["current_lossless_source_v0_probe_min_bitrate"],
                expected_probe,
            )


class TestGeneratedHaveAnalysisOutcomeContracts(unittest.TestCase):
    @given(
        failure=st.sampled_from((
            ("neutral analyser error", "stale", "snapshot_changed"),
            ("missing path", None, "path_missing"),
            ("PermissionError: denied", "matched", "permission_denied"),
            ("no audio files", "matched", "no_audio_files"),
            ("ffprobe crashed", "matched", "analyser_failure"),
        )),
        requeue=st.booleans(),
        cooldown_verdict=st.booleans(),
    )
    def test_taxonomy_audit_lifecycle_and_cooldown_survive(
        self,
        failure: tuple[str, str | None, str],
        requeue: bool,
        cooldown_verdict: bool,
    ) -> None:
        raw_error, snapshot_guard, expected_category = failure
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="downloading"))
        db.set_cooldown_result(cooldown_verdict)
        cooled: set[str] = set()
        result = _record_have_analysis_error(
            make_dispatch_request(
                request_id=42,
                path="/generated/candidate",
                dl_info=DownloadInfo(
                    username="generated-peer",
                    contributor_usernames=("generated-contributor",),
                    filetype="flac",
                ),
                requeue_on_failure=requeue,
                candidate_download_log_id=91,
                cooled_down_users=cooled,
            ),
            db,
            raw_error=raw_error,
            installed_path="/generated/library",
            snapshot_guard=snapshot_guard,
        )
        self.assertIsInstance(result, int)
        audit = db.download_logs[-1]
        payload = msgspec.json.decode(
            audit.validation_result or "{}",
            type=HaveAnalysisFailure,
        )
        self.assertEqual(payload.failure_category, expected_category)
        self.assertEqual(payload.error, raw_error)
        self.assertEqual(payload.installed_path, "/generated/library")
        self.assertEqual(payload.candidate_reference, "/generated/candidate")
        self.assertEqual(audit.candidate_contributor_usernames, ["generated-contributor"])
        self.assertEqual(audit.filetype, "flac")
        self.assertEqual(audit.extra["download_path"], "/generated/library")
        self.assertEqual(audit.staged_path, "/generated/candidate")
        self.assertEqual(audit.source_download_log_id, 91)
        self.assertEqual(db.request(42)["status"], "wanted" if requeue else "downloading")
        self.assertEqual(db.request(42)["validation_attempts"], 1 if requeue else 0)
        self.assertEqual(db.cooldowns_applied, ["generated-peer"])
        self.assertEqual(
            cooled,
            {"generated-peer"} if cooldown_verdict else set(),
        )


if __name__ == "__main__":
    unittest.main()
