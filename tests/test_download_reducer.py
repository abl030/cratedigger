"""Tests for download state reducer — pure decision function."""

import unittest
from datetime import datetime, timezone

from lib.quality import (
    ActiveDownloadFileState,
    ActiveDownloadState,
    DownloadDecision,
    DownloadVerdict,
    PollCycleConfig,
    PollCycleDecision,
    PollCycleSnapshot,
    PollFileSnapshot,
    decide_download_action,
    reduce_poll_cycle,
)


class TestDecideDownloadAction(unittest.TestCase):
    """Test the pure download decision function."""

    def _decide(self, **overrides):
        """Build default args and apply overrides."""
        defaults = dict(
            album_done=False,
            error_filenames=None,
            total_files=3,
            all_remote_queued=False,
            elapsed_seconds=60.0,
            idle_seconds=10.0,
            remote_queue_timeout=3600,
            stalled_timeout=1800,
            file_retries={},
            max_file_retries=5,
            processing_started=False,
        )
        defaults.update(overrides)
        return decide_download_action(**defaults)  # type: ignore[arg-type]

    def test_processing_started(self):
        v = self._decide(processing_started=True)
        self.assertEqual(v.decision, DownloadDecision.processing)

    def test_complete_no_errors(self):
        v = self._decide(album_done=True, error_filenames=None)
        self.assertEqual(v.decision, DownloadDecision.complete)

    def test_remote_queue_timeout(self):
        v = self._decide(all_remote_queued=True,
                         elapsed_seconds=3601, remote_queue_timeout=3600)
        self.assertEqual(v.decision, DownloadDecision.timeout_remote_queue)

    def test_remote_queue_not_timed_out(self):
        v = self._decide(all_remote_queued=True,
                         elapsed_seconds=1800, remote_queue_timeout=3600)
        self.assertEqual(v.decision, DownloadDecision.in_progress)

    def test_all_files_errored(self):
        v = self._decide(error_filenames=["a.flac", "b.flac", "c.flac"],
                         total_files=3)
        self.assertEqual(v.decision, DownloadDecision.timeout_all_errored)

    def test_partial_errors_retries_left(self):
        v = self._decide(error_filenames=["a.flac"],
                         file_retries={"a.flac": 2},
                         max_file_retries=5)
        self.assertEqual(v.decision, DownloadDecision.retry_files)
        self.assertEqual(v.files_to_retry, ["a.flac"])

    def test_partial_errors_max_retries(self):
        v = self._decide(error_filenames=["a.flac"],
                         file_retries={"a.flac": 5},
                         max_file_retries=5)
        self.assertEqual(v.decision, DownloadDecision.timeout_stalled)
        self.assertIn("retry limit", v.reason)

    def test_stalled_timeout(self):
        v = self._decide(idle_seconds=1801, stalled_timeout=1800)
        self.assertEqual(v.decision, DownloadDecision.timeout_stalled)
        self.assertIn("no download progress", v.reason)

    def test_stalled_not_checked_when_remote_queued(self):
        """Stall timer doesn't apply when all files are remotely queued."""
        v = self._decide(all_remote_queued=True,
                         idle_seconds=9999, stalled_timeout=1800,
                         elapsed_seconds=100, remote_queue_timeout=3600)
        self.assertEqual(v.decision, DownloadDecision.in_progress)

    def test_in_progress(self):
        v = self._decide()
        self.assertEqual(v.decision, DownloadDecision.in_progress)

    def test_multiple_retries_only_returns_eligible(self):
        """Only files below max_retries are in files_to_retry."""
        v = self._decide(
            error_filenames=["a.flac", "b.flac"],
            file_retries={"a.flac": 5, "b.flac": 2},
            max_file_retries=5,
        )
        # a.flac is at limit → timeout
        self.assertEqual(v.decision, DownloadDecision.timeout_stalled)

    def test_error_file_not_in_retries_dict(self):
        """File with no retry history → 0 retries, should retry."""
        v = self._decide(error_filenames=["new.flac"],
                         file_retries={})
        self.assertEqual(v.decision, DownloadDecision.retry_files)
        self.assertEqual(v.files_to_retry, ["new.flac"])


class TestDownloadDecisionEnum(unittest.TestCase):
    def test_all_values(self):
        self.assertEqual(len(DownloadDecision), 7)


class TestReducePollCycle(unittest.TestCase):
    """The poll-cycle reducer owns every persisted-state transition."""

    NOW = datetime(2026, 7, 11, 3, 0, tzinfo=timezone.utc)

    def _state(self, **overrides):
        values = dict(
            filetype="flac",
            enqueued_at="2026-07-11T02:58:00+00:00",
            last_progress_at="2026-07-11T02:59:00+00:00",
            files=[
                ActiveDownloadFileState(
                    username="alice",
                    filename="Album\\01.flac",
                    file_dir="Album",
                    size=100,
                ),
            ],
        )
        values.update(overrides)
        return ActiveDownloadState(**values)  # type: ignore[arg-type]

    def _snapshot(self, *files, **overrides):
        values = dict(files=list(files))
        values.update(overrides)
        return PollCycleSnapshot(**values)  # type: ignore[arg-type]

    def _reduce(self, state, snapshot, **cfg_overrides):
        cfg_values = dict(
            remote_queue_timeout=300,
            stalled_timeout=180,
            max_file_retries=5,
            vanished_grace_seconds=60,
        )
        cfg_values.update(cfg_overrides)
        return reduce_poll_cycle(
            state,
            snapshot,
            self.NOW,
            PollCycleConfig(**cfg_values),
        )

    def test_missing_state_requests_crash_recovery(self):
        result = self._reduce(None, self._snapshot())

        self.assertIsNone(result.state)
        self.assertEqual(
            result.verdict.decision,
            PollCycleDecision.reset_missing_state,
        )

    def test_active_import_job_gates_before_processing_recovery(self):
        state = self._state(processing_started_at="2026-07-11T02:59:30+00:00")
        result = self._reduce(
            state,
            self._snapshot(
                active_import_job_id=17,
                active_import_job_status="running",
                processing_blocked_reason="multiple_populated_paths",
            ),
        )

        self.assertEqual(result.state, state)
        self.assertEqual(
            result.verdict.decision,
            PollCycleDecision.wait_import_job,
        )
        self.assertEqual(result.verdict.import_job_id, 17)
        self.assertEqual(result.verdict.import_job_status, "running")

    def test_processing_recovery_returns_selected_complete_state(self):
        state = self._state(
            processing_started_at="2026-07-11T02:59:30+00:00",
            current_path="/old",
        )
        result = self._reduce(
            state,
            self._snapshot(processing_current_path="/recovered"),
        )

        assert result.state is not None
        self.assertEqual(result.state.current_path, "/recovered")
        self.assertEqual(
            result.verdict.decision,
            PollCycleDecision.processing,
        )

    def test_processing_recovery_block_leaves_state_actionable(self):
        state = self._state(
            processing_started_at="2026-07-11T02:59:30+00:00",
            current_path="/persisted",
        )
        result = self._reduce(
            state,
            self._snapshot(processing_blocked_reason="legacy_shared_only"),
        )

        self.assertEqual(result.state, state)
        self.assertEqual(
            result.verdict.decision,
            PollCycleDecision.wait_processing_recovery,
        )
        self.assertEqual(result.verdict.reason, "legacy_shared_only")

    def test_progress_snapshot_returns_new_state_without_mutating_input(self):
        state = self._state()
        observed = PollFileSnapshot(
            transfer_id="tx-1",
            state="InProgress",
            bytes_transferred=40,
        )

        result = self._reduce(state, self._snapshot(observed))

        assert result.state is not None
        self.assertEqual(state.files[0].bytes_transferred, 0)
        self.assertIsNone(state.files[0].last_state)
        self.assertEqual(result.state.files[0].bytes_transferred, 40)
        self.assertEqual(result.state.files[0].last_state, "InProgress")
        self.assertEqual(result.state.last_progress_at, self.NOW.isoformat())
        self.assertEqual(
            result.verdict.decision,
            PollCycleDecision.in_progress,
        )

    def test_terminal_failure_is_restored_when_snapshot_drops_row(self):
        state = self._state(files=[
            ActiveDownloadFileState(
                username="alice",
                filename="Album\\01.flac",
                file_dir="Album",
                size=100,
                last_state="Completed, Rejected",
                last_exception="peer banned us",
            ),
        ])

        result = self._reduce(state, self._snapshot(PollFileSnapshot()))

        assert result.state is not None
        self.assertEqual(
            result.state.files[0].last_state,
            "Completed, Rejected",
        )
        self.assertEqual(result.state.files[0].last_exception, "peer banned us")
        self.assertEqual(
            result.verdict.decision,
            PollCycleDecision.timeout_all_errored,
        )

    def test_fresh_terminal_failure_is_returned_with_its_exception(self):
        state = self._state(files=[
            ActiveDownloadFileState(
                username="alice",
                filename="Album\\01.flac",
                file_dir="Album",
                size=100,
                last_state="InProgress",
            ),
        ])

        result = self._reduce(
            state,
            self._snapshot(PollFileSnapshot(
                transfer_id="tx-1",
                state="Completed, Rejected",
                exception="Transfer rejected: Banned",
            )),
        )

        assert result.state is not None
        self.assertEqual(result.state.files[0].last_state, "Completed, Rejected")
        self.assertEqual(
            result.state.files[0].last_exception,
            "Transfer rejected: Banned",
        )
        self.assertEqual(state.files[0].last_state, "InProgress")

    def test_fresh_all_vanished_waits_without_fabricating_evidence(self):
        state = self._state(
            enqueued_at="2026-07-11T02:59:30+00:00",
            last_progress_at="2026-07-11T02:59:30+00:00",
        )

        result = self._reduce(state, self._snapshot(PollFileSnapshot()))

        self.assertEqual(result.state, state)
        self.assertEqual(
            result.verdict.decision,
            PollCycleDecision.wait_fresh_vanished,
        )

    def test_old_all_vanished_times_out_without_fabricating_evidence(self):
        state = self._state()

        result = self._reduce(state, self._snapshot(PollFileSnapshot()))

        self.assertEqual(result.state, state)
        self.assertEqual(
            result.verdict.decision,
            PollCycleDecision.timeout_vanished,
        )

    def test_partial_vanish_is_captured_and_retried_without_losing_evidence(self):
        files = [
            ActiveDownloadFileState(
                username="alice",
                filename="Album\\01.flac",
                file_dir="Album",
                size=100,
            ),
            ActiveDownloadFileState(
                username="alice",
                filename="Album\\02.flac",
                file_dir="Album",
                size=100,
            ),
        ]
        state = self._state(files=files)
        result = self._reduce(
            state,
            self._snapshot(
                PollFileSnapshot(
                    transfer_id="tx-1",
                    state="InProgress",
                    bytes_transferred=25,
                ),
                PollFileSnapshot(),
            ),
        )

        assert result.state is not None
        vanished = result.state.files[1]
        self.assertEqual(vanished.last_state, "Completed, Errored")
        self.assertEqual(vanished.retry_count, 1)
        self.assertEqual(
            result.verdict.decision,
            PollCycleDecision.retry_files,
        )
        self.assertEqual(result.verdict.files_to_retry, ["Album\\02.flac"])

    def test_complete_marks_processing_before_dispatch(self):
        state = self._state()
        result = self._reduce(
            state,
            self._snapshot(
                PollFileSnapshot(
                    transfer_id="tx-1",
                    state="Completed, Succeeded",
                    bytes_transferred=100,
                ),
                completion_current_path="/canonical",
            ),
        )

        assert result.state is not None
        self.assertEqual(result.state.processing_started_at, self.NOW.isoformat())
        self.assertEqual(result.state.current_path, "/canonical")
        self.assertEqual(
            result.verdict.decision,
            PollCycleDecision.complete,
        )

    def test_timeout_branches_delegate_to_existing_action_policy(self):
        cases = [
            (
                "remote queue",
                self._state(enqueued_at="2026-07-11T02:50:00+00:00"),
                PollFileSnapshot(
                    transfer_id="tx-1",
                    state="Queued, Remotely",
                ),
                PollCycleDecision.timeout_remote_queue,
            ),
            (
                "stalled",
                self._state(
                    enqueued_at="2026-07-11T02:50:00+00:00",
                    last_progress_at="2026-07-11T02:50:00+00:00",
                    files=[ActiveDownloadFileState(
                        username="alice",
                        filename="Album\\01.flac",
                        file_dir="Album",
                        size=100,
                        last_state="InProgress",
                    )],
                ),
                PollFileSnapshot(
                    transfer_id="tx-1",
                    state="InProgress",
                ),
                PollCycleDecision.timeout_stalled,
            ),
        ]
        for desc, state, file_snapshot, expected in cases:
            with self.subTest(desc=desc):
                result = self._reduce(
                    state,
                    self._snapshot(file_snapshot),
                )
                self.assertEqual(result.verdict.decision, expected)

    def test_retry_limit_timeout_preserves_last_terminal_evidence(self):
        state = self._state(files=[
            ActiveDownloadFileState(
                username="alice",
                filename="Album\\01.flac",
                file_dir="Album",
                size=100,
                retry_count=5,
            ),
            ActiveDownloadFileState(
                username="alice",
                filename="Album\\02.flac",
                file_dir="Album",
                size=100,
            ),
        ])
        result = self._reduce(
            state,
            self._snapshot(
                PollFileSnapshot(
                    transfer_id="tx-1",
                    state="Completed, Rejected",
                    exception="banned",
                ),
                PollFileSnapshot(
                    transfer_id="tx-2",
                    state="InProgress",
                ),
            ),
        )

        assert result.state is not None
        self.assertEqual(
            result.verdict.decision,
            PollCycleDecision.timeout_stalled,
        )
        self.assertEqual(result.state.files[0].last_exception, "banned")


if __name__ == "__main__":
    unittest.main()
