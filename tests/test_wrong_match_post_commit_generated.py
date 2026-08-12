"""Generated patrol for automation Wrong Matches post-commit triage."""

from __future__ import annotations

import os
import tempfile
import unittest
from unittest.mock import patch

from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401
from lib.dispatch import (
    DispatchOutcome,
    _record_rejection_and_maybe_requeue,
)
from lib.import_execution import ExecutionLeaseSnapshot, ProcessIdentity
from lib.quality import ActiveDownloadState, DownloadInfo, ValidationResult
from lib.quality_evidence import snapshot_audio_files
from lib.terminal_outcomes import PendingImportTerminalOutcome
from lib.wrong_match_policy import (
    DELETE_ELIGIBLE_REJECTION_SCENARIOS,
    WRONG_MATCH_EXCLUDED_REJECTION_SCENARIOS,
)
from tests.fakes import FakePipelineDB
from tests.helpers import (
    claim_next_import_job,
    claim_next_import_preview_job,
    finalize_claimed_dispatch,
    handoff_automation_owner,
    make_album_quality_evidence,
    make_request_row,
)

_REQUEST_ID = 42
_RELEASE_ID = "generated-wrong-match-release"
# The four delete-eligible scenarios (D6 allowlist), plus a mix of visible-
# but-not-delete-eligible world failures (untracked_audio, request_missing_*)
# and quality/fact rejects that never even carry a failed_path — every one of
# these must reach post-commit ONLY if it is in the allowlist.
_CANDIDATE_SCENARIOS = (
    *DELETE_ELIGIBLE_REJECTION_SCENARIOS,
    "strong_mismatch",
    "downgrade",
    "transcode_downgrade",
    "untracked_audio",
    "request_missing_mbid",
    "request_missing_request_id",
)
_SCENARIOS = tuple(sorted(
    {*_CANDIDATE_SCENARIOS, *WRONG_MATCH_EXCLUDED_REJECTION_SCENARIOS},
))


def assert_wrong_match_cleanup_reaches_post_commit(
    *,
    scenario: str,
    cleanup_calls: list[tuple[str, str]],
) -> None:
    """Only a delete-eligible reject reaches the cleanup reducer (#1077, D6).

    Everything else — world failures with a reviewable folder, quality/fact
    rejects, and every unknown string — cleans once at post-commit only in
    the sense of running the terminal bundle; the destructive reducer itself
    must never be consulted.
    """
    expected_calls = (
        [("wanted", "failed")]
        if scenario in DELETE_ELIGIBLE_REJECTION_SCENARIOS
        else []
    )
    if expected_calls and not cleanup_calls:
        raise AssertionError(
            "delete-eligible automation rejection bypassed post-commit "
            f"Wrong Matches triage: scenario={scenario!r} "
            f"calls={cleanup_calls!r}"
        )
    if not expected_calls and cleanup_calls:
        raise AssertionError(
            "delete-ineligible automation rejection reached post-commit "
            f"Wrong Matches cleanup: scenario={scenario!r} "
            f"calls={cleanup_calls!r}"
        )
    if cleanup_calls != expected_calls:
        raise AssertionError(
            "unexpected post-commit cleanup shape: "
            f"scenario={scenario!r} calls={cleanup_calls!r}"
        )


def _execution_lease(lane: str) -> ExecutionLeaseSnapshot:
    return ExecutionLeaseSnapshot(
        host_boot_id="generated-wrong-match-boot",
        invocation_id=f"generated-wrong-match-{lane}",
        systemd_unit=f"cratedigger-{lane}.service",
        worker=ProcessIdentity(
            pid=1001 if lane == "preview" else 1002,
            start_ticks=2001 if lane == "preview" else 2002,
        ),
    )


def _run_automation_rejection(
    *,
    scenario: str,
    file_count: int,
) -> list[tuple[str, str]]:
    with tempfile.TemporaryDirectory() as root:
        processing_albums = os.path.join(root, "processing", "albums")
        canonical_path = os.path.join(processing_albums, "candidate")
        os.makedirs(canonical_path)
        for index in range(file_count):
            with open(
                os.path.join(canonical_path, f"{index:02d}.mp3"),
                "wb",
            ) as handle:
                handle.write(f"track-{index}".encode())

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=_REQUEST_ID,
            status="wanted",
            mb_release_id=_RELEASE_ID,
        ))
        owner = handoff_automation_owner(
            db,
            _REQUEST_ID,
            state=ActiveDownloadState(
                filetype="mp3",
                enqueued_at="2026-07-30T13:13:58+00:00",
                files=[],
                current_path=canonical_path,
            ).to_json(),
            canonical_path=canonical_path,
        )
        preview_lease = _execution_lease("preview")
        preview_job = claim_next_import_preview_job(
            db,
            worker_id="generated-preview",
            execution_lease=preview_lease,
        )
        assert preview_job is not None and preview_job.id == owner.id

        evidence = make_album_quality_evidence(
            mb_release_id=_RELEASE_ID,
            source_path=canonical_path,
            files=snapshot_audio_files(canonical_path),
        )
        db.upsert_album_quality_evidence(evidence)
        persisted = db.find_album_quality_evidence(
            mb_release_id=_RELEASE_ID,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert persisted is not None and persisted.id is not None
        assert db.set_import_job_candidate_evidence(
            owner.id,
            persisted.id,
            expected_execution_lease=preview_lease,
        )
        assert db.mark_import_job_preview_importable(
            owner.id,
            preview_result={"ready": True},
            expected_execution_lease=preview_lease,
        ) is not None

        import_lease = _execution_lease("import")
        claimed = claim_next_import_job(
            db,
            worker_id="generated-import",
            execution_lease=import_lease,
        )
        assert claimed is not None and claimed.id == owner.id

        wrong_matches = os.path.join(processing_albums, "wrong_matches")
        os.makedirs(wrong_matches)
        failed_path = os.path.join(wrong_matches, "candidate")
        os.rename(canonical_path, failed_path)

        validation = ValidationResult(
            valid=False,
            distance=0.1697,
            scenario=scenario,
            detail=f"generated {scenario}",
            failed_path=failed_path,
        )
        pending = _record_rejection_and_maybe_requeue(
            db,  # pyright: ignore[reportArgumentType]
            _REQUEST_ID,
            DownloadInfo(filetype="mp3", username="generated-peer"),
            detail=validation.detail,
            error=None,
            validation_result=validation.to_json(),
            requeue=True,
            import_job_id=claimed.id,
        )
        assert isinstance(pending, PendingImportTerminalOutcome)
        outcome = DispatchOutcome(
            success=False,
            message=f"Rejected: {scenario}",
            terminal_outcome=pending,
            post_commit_wrong_match_scenario=scenario,
        )
        calls: list[tuple[str, str]] = []

        def observe_cleanup(
            db_arg,
            _download_log_id: int,
            *,
            ignore_import_job_id: int | None,
        ) -> None:
            assert ignore_import_job_id == claimed.id
            terminal_job = db_arg.get_import_job(claimed.id)
            assert terminal_job is not None
            calls.append((
                str(db_arg.request(_REQUEST_ID)["status"]),
                terminal_job.status,
            ))

        with patch(
            "lib.wrong_match_cleanup_service.cleanup_wrong_match",
            side_effect=observe_cleanup,
        ):
            finalize_claimed_dispatch(db, claimed, outcome)
        return calls


class TestWrongMatchPostCommitGenerated(unittest.TestCase):
    @given(
        scenario=st.sampled_from(_SCENARIOS),
        file_count=st.integers(min_value=1, max_value=4),
    )
    @example(scenario="high_distance", file_count=14)
    def test_every_automation_rejection_reaches_eligible_post_commit_triage(
        self,
        *,
        scenario: str,
        file_count: int,
    ) -> None:
        assert_wrong_match_cleanup_reaches_post_commit(
            scenario=scenario,
            cleanup_calls=_run_automation_rejection(
                scenario=scenario,
                file_count=file_count,
            ),
        )

    def test_checker_rejects_known_bad_automation_early_return(self) -> None:
        with self.assertRaisesRegex(
            AssertionError,
            "bypassed post-commit",
        ):
            assert_wrong_match_cleanup_reaches_post_commit(
                scenario="high_distance",
                cleanup_calls=[],
            )

    def test_checker_rejects_delete_ineligible_scenario_reaching_cleanup(
        self,
    ) -> None:
        """#1077, D6: a world failure must never reach the reducer."""
        with self.assertRaisesRegex(
            AssertionError,
            "delete-ineligible",
        ):
            assert_wrong_match_cleanup_reaches_post_commit(
                scenario="untracked_audio",
                cleanup_calls=[("wanted", "failed")],
            )
