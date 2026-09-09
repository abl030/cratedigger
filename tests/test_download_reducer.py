"""Tests for download state reducer — pure decision function."""

import unittest
from datetime import UTC, datetime
from typing import ClassVar

from lib.quality import (
    ActiveDownloadFileState,
    ActiveDownloadState,
    DownloadDecision,
    PollCycleConfig,
    PollCycleDecision,
    PollCycleSnapshot,
    PollFileSnapshot,
    decide_download_action,
    reduce_poll_cycle,
)


def _seeded_file(**overrides) -> ActiveDownloadFileState:
    """A persisted file with its identity seeded, not left at defaults.

    ``local_path`` and the disc numbering are the per-file half of what a
    poll must carry and never re-derive (#1405 review): a fixture that
    leaves them ``None`` cannot tell a copy that carries them from one
    that drops them. Module-level because
    ``TestReducePollCycle.IDENTITY_BRANCH_WORLDS`` is evaluated while its
    class is still being defined.
    """
    values = {
        "username": "alice",
        "filename": "Album\\01.flac",
        "file_dir": "Album",
        "size": 100,
        "disk_no": 1,
        "disk_count": 2,
        "local_path": "/downloads/Album/01.flac",
    }
    values.update(overrides)
    return ActiveDownloadFileState(**values)


class TestDecideDownloadAction(unittest.TestCase):
    """Test the pure download decision function."""

    def _decide(self, **overrides):
        """Build default args and apply overrides."""
        defaults = {
            "album_done": False,
            "error_filenames": None,
            "total_files": 3,
            "all_remote_queued": False,
            "elapsed_seconds": 60.0,
            "idle_seconds": 10.0,
            "remote_queue_timeout": 3600,
            "stalled_timeout": 1800,
            "file_retries": {},
            "max_file_retries": 5,
            "processing_started": False,
        }
        defaults.update(overrides)
        return decide_download_action(**defaults)

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
        # The reason is the operator-facing failure evidence: the poller
        # hands it straight to _timeout_album, which records it as the
        # download_log message for this timeout. Its two siblings below
        # assert their own reasons; this one did not, so nulling it
        # survived the suite (#1405 mutmut breadth pass).
        self.assertIn("remote_queue_timeout", v.reason)

    def test_remote_queue_timeout_fires_exactly_at_the_timeout(self):
        """The boundary second belongs to the timeout, not to waiting.

        #812 was a tie comparison off by exactly this much.
        """
        v = self._decide(all_remote_queued=True,
                         elapsed_seconds=3600, remote_queue_timeout=3600)
        self.assertEqual(v.decision, DownloadDecision.timeout_remote_queue)

    def test_stall_fires_exactly_at_the_timeout(self):
        """Same boundary, the other timer."""
        v = self._decide(idle_seconds=1800, stalled_timeout=1800)
        self.assertEqual(v.decision, DownloadDecision.timeout_stalled)

    def test_a_file_with_no_retry_history_gets_its_whole_budget(self):
        """A fresh error is retry zero, so one retry is still allowed.

        With ``max_file_retries=1`` the default decides the outcome:
        zero retries used means retry, one means give up.
        """
        v = self._decide(error_filenames=["new.flac"], total_files=2,
                         file_retries={}, max_file_retries=1)
        self.assertEqual(v.decision, DownloadDecision.retry_files)
        self.assertEqual(v.files_to_retry, ["new.flac"])

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

    NOW = datetime(2026, 7, 11, 3, 0, tzinfo=UTC)

    def _state(self, **overrides):
        values = {
            "filetype": "flac",
            "enqueued_at": "2026-07-11T02:58:00+00:00",
            "last_progress_at": "2026-07-11T02:59:00+00:00",
            "files": [_seeded_file()],
            # #1196 item 1: present by default so every case in this
            # class builds a state with a real fingerprint value. This
            # does NOT by itself guard against a reducer path dropping
            # the field -- only the cases that actually READ
            # ``result.state.attempt_fingerprint`` fail when a mutant
            # drops it from ``_copy_download_state``. Those are
            # ``test_progress_snapshot_carries_attempt_fingerprint_
            # forward`` (the #1196 single-branch pin) and
            # ``test_every_branch_carries_the_attempt_identity_forward``
            # (#1405, every stateful branch, both identity fields).
            # Every other case here stays green because none of them
            # read either field.
            "attempt_fingerprint": "fp-abc12345",
        }
        values.update(overrides)
        return ActiveDownloadState(**values)

    def _snapshot(self, *files, **overrides):
        values = {"files": list(files)}
        values.update(overrides)
        return PollCycleSnapshot(**values)

    def _reduce(self, state, snapshot, **cfg_overrides):
        cfg_values = {
            "remote_queue_timeout": 300,
            "stalled_timeout": 180,
            "max_file_retries": 5,
            "vanished_grace_seconds": 60,
        }
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

    def test_progress_snapshot_carries_attempt_fingerprint_forward(self):
        """#1196 item 1: the attempt's identity never changes across a
        poll-cycle progress update. A reducer rebuild that dropped the
        field (e.g. reconstructing via a bare ``ActiveDownloadState(...)``
        instead of ``_copy_download_state``) would silently erase it from
        ``active_download_state`` on the very first poll cycle after
        claim -- this pin fails closed against that regression."""
        state = self._state(attempt_fingerprint="fp-9f8e7d6c")
        observed = PollFileSnapshot(
            transfer_id="tx-1", state="InProgress", bytes_transferred=40,
        )

        result = self._reduce(state, self._snapshot(observed))

        assert result.state is not None
        self.assertEqual(result.state.attempt_fingerprint, "fp-9f8e7d6c")

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

    def test_the_vanished_grace_window_closes_exactly_at_its_deadline(self):
        """The boundary second is outside the grace, like both timeouts.

        One second earlier the reducer must still wait; at the deadline
        the planned-but-invisible attempt has had its window.
        """
        cases = [
            ("one second inside", "2026-07-11T02:59:01+00:00",
             PollCycleDecision.wait_fresh_vanished),
            ("exactly at the deadline", "2026-07-11T02:59:00+00:00",
             PollCycleDecision.timeout_vanished),
        ]
        for desc, enqueued_at, expected in cases:
            with self.subTest(desc=desc):
                result = self._reduce(
                    self._state(
                        enqueued_at=enqueued_at,
                        last_progress_at=enqueued_at),
                    self._snapshot(PollFileSnapshot()),
                    vanished_grace_seconds=60,
                )
                self.assertEqual(result.verdict.decision, expected)

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

    def test_complete_leaves_processing_publication_to_atomic_handoff(self):
        state = self._state()
        result = self._reduce(
            state,
            self._snapshot(
                PollFileSnapshot(
                    transfer_id="tx-1",
                    state="Completed, Succeeded",
                    bytes_transferred=100,
                ),
            ),
        )

        assert result.state is not None
        self.assertIsNone(result.state.processing_started_at)
        self.assertIsNone(result.state.current_path)
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

    #: One world per ``PollCycleDecision`` the reducer can return on a
    #: state that is still ``downloading``. ``reset_missing_state`` is
    #: absent because it is the one branch whose input state is ``None``;
    #: it gets its own assertion below. Each row is
    #: ``(decision, state overrides, per-file snapshots)``, reduced
    #: under this class's one config.
    #:
    #: The two vanished rows are CONTROLS, not guards: on those branches
    #: the reducer returns the persisted object itself, so their
    #: assertions compare an input to itself and no rebuild mutant can
    #: fail them. They are here to prove the branch reaches the
    #: assertions at all. The other six rebuild the state, and are where
    #: every mutant this test kills is killed.
    IDENTITY_BRANCH_WORLDS: ClassVar[tuple[tuple[
        PollCycleDecision,
        dict[str, object],
        list[PollFileSnapshot],
    ], ...]] = (
        (
            PollCycleDecision.wait_fresh_vanished,
            {"enqueued_at": "2026-07-11T02:59:30+00:00"},
            [PollFileSnapshot()],
        ),
        (
            PollCycleDecision.timeout_vanished,
            {},
            [PollFileSnapshot()],
        ),
        (
            PollCycleDecision.in_progress,
            {},
            [PollFileSnapshot(
                transfer_id="tx-1", state="InProgress", bytes_transferred=40)],
        ),
        (
            PollCycleDecision.complete,
            {},
            [PollFileSnapshot(
                transfer_id="tx-1",
                state="Completed, Succeeded",
                bytes_transferred=100,
            )],
        ),
        (
            PollCycleDecision.retry_files,
            {"files": [
                _seeded_file(),
                _seeded_file(
                    filename="Album\\02.flac",
                    local_path="/downloads/Album/02.flac"),
            ]},
            [
                PollFileSnapshot(
                    transfer_id="tx-1", state="InProgress",
                    bytes_transferred=25),
                PollFileSnapshot(
                    transfer_id="tx-2", state="Completed, Rejected",
                    exception="banned"),
            ],
        ),
        (
            PollCycleDecision.timeout_remote_queue,
            {"enqueued_at": "2026-07-11T02:50:00+00:00"},
            [PollFileSnapshot(transfer_id="tx-1", state="Queued, Remotely")],
        ),
        (
            PollCycleDecision.timeout_stalled,
            {
                "enqueued_at": "2026-07-11T02:50:00+00:00",
                "last_progress_at": "2026-07-11T02:50:00+00:00",
                "files": [_seeded_file(last_state="InProgress")],
            },
            [PollFileSnapshot(transfer_id="tx-1", state="InProgress")],
        ),
        (
            PollCycleDecision.timeout_all_errored,
            {"files": [_seeded_file(
                last_state="Completed, Rejected",
                last_exception="banned")]},
            [PollFileSnapshot()],
        ),
    )

    def test_every_branch_carries_the_attempt_identity_forward(self):
        """#1405: every reducer rebuild preserves the attempt's identity.

        ``attempt_fingerprint`` (#1196 item 1) and ``search_log_id``
        (#811) are set once per attempt and never re-derived from a
        poll observation. Every rebuild in ``reduce_poll_cycle`` goes
        through ``_copy_download_state``, and the first
        ``update_download_state_if_downloading`` after a claim rewrites
        the whole state from what that helper returns -- so a field the
        helper forgets is erased from ``active_download_state`` on the
        very first poll cycle. That is exactly what #1405 measured in
        production for ``search_log_id`` (request 4351, search_log
        563143, download_log 41283 with a NULL link), while
        ``attempt_fingerprint``'s own protection was a comment plus one
        single-branch pin.

        The same holds one level down and for the fields no issue is
        named after: ``filetype``, ``enqueued_at``, the slskd queue key,
        the disc numbering, and the event-stamped ``local_path`` that is
        the ONLY completed-file location authority. Every row seeds them,
        because a guard over a field the fixture leaves at its default
        cannot tell "carried" from "dropped" (#1405 review).

        One row per non-``None``-state decision, so a branch that starts
        rebuilding state through some other constructor is caught here
        rather than in production.
        """
        for decision, overrides, snapshots in self.IDENTITY_BRANCH_WORLDS:
            with self.subTest(decision=decision.value):
                state = self._state(
                    attempt_fingerprint="fp-9f8e7d6c",
                    search_log_id=563143,
                    **overrides,
                )
                result = self._reduce(state, self._snapshot(*snapshots))

                self.assertEqual(result.verdict.decision, decision)
                assert result.state is not None
                self.assertEqual(
                    result.state.attempt_fingerprint, "fp-9f8e7d6c")
                self.assertEqual(result.state.search_log_id, 563143)
                # The rest of the attempt's identity, at both levels.
                self.assertEqual(result.state.filetype, state.filetype)
                self.assertEqual(result.state.enqueued_at, state.enqueued_at)
                self.assertEqual(
                    len(result.state.files), len(state.files))
                for index, was in enumerate(state.files):
                    now = result.state.files[index]
                    self.assertEqual(now.username, was.username)
                    self.assertEqual(now.filename, was.filename)
                    self.assertEqual(now.file_dir, was.file_dir)
                    self.assertEqual(now.size, was.size)
                    self.assertEqual(now.disk_no, was.disk_no)
                    self.assertEqual(now.disk_count, was.disk_count)
                    self.assertEqual(now.local_path, was.local_path)

    def test_the_identity_branch_table_covers_every_stateful_decision(self):
        """A new decision branch owes a row above, not a silent gap."""
        covered = {row[0] for row in self.IDENTITY_BRANCH_WORLDS}
        self.assertEqual(
            covered,
            set(PollCycleDecision) - {PollCycleDecision.reset_missing_state},
        )

    def test_the_reset_branch_carries_no_identity_because_it_has_no_state(self):
        """The one decision whose state really is ``None`` (control)."""
        result = self._reduce(None, self._snapshot())

        self.assertEqual(
            result.verdict.decision, PollCycleDecision.reset_missing_state)
        self.assertIsNone(result.state)

    def test_a_state_change_with_no_new_bytes_still_restarts_the_stall_clock(
        self,
    ):
        """Progress is a state transition too, not only bytes.

        A peer that moves a file from unobserved to ``InProgress``
        without yet delivering a byte IS making progress, and the
        reducer's own progress test says so
        (``current_state != file.last_state`` AND the new state is not
        one of the non-progress states). Only the byte half of that
        ``or`` was ever asserted, so inverting the state half to ``in
        _NON_PROGRESS_STATES`` survived the whole reducer suite and both
        generated properties -- while flipping this world's decision
        from ``in_progress`` to ``timeout_stalled``, i.e. cancelling and
        requeuing a download that is fine. Found by the #1405 mutmut
        breadth pass.
        """
        state = self._state(
            enqueued_at="2026-07-11T02:50:00+00:00",
            last_progress_at="2026-07-11T02:50:00+00:00",
        )

        result = self._reduce(
            state,
            self._snapshot(PollFileSnapshot(
                transfer_id="tx-1", state="InProgress",
                bytes_transferred=0)),
        )

        assert result.state is not None
        self.assertEqual(result.state.last_progress_at, self.NOW.isoformat())
        self.assertEqual(
            result.verdict.decision, PollCycleDecision.in_progress)

    def test_an_observation_with_no_state_at_all_is_not_progress(self):
        """The other must-still-work control: absence is not evidence.

        A transfer slskd lists without any state, against a file we have
        never seen a state for, says nothing about progress -- so the
        stall clock keeps running. The reducer spells that as the empty
        fallback ``(current_state or "")`` landing in the non-progress
        set; a mutant that makes the fallback any other string turns
        silence into progress and postpones every stall.
        """
        state = self._state(
            enqueued_at="2026-07-11T02:50:00+00:00",
            last_progress_at="2026-07-11T02:50:00+00:00",
        )

        result = self._reduce(
            state,
            self._snapshot(PollFileSnapshot(
                transfer_id="tx-1", state=None, bytes_transferred=0)),
        )

        assert result.state is not None
        self.assertEqual(
            result.state.last_progress_at, "2026-07-11T02:50:00+00:00")
        self.assertEqual(
            result.verdict.decision, PollCycleDecision.timeout_stalled)

    def test_a_zero_byte_observation_replaces_a_larger_persisted_count(self):
        """Zero is an observation, not a missing value.

        ``_copy_download_file_state`` reads ``None`` as "leave this field
        alone", so the guard has to be ``is not None`` rather than plain
        truthiness: a live transfer reporting 0 bytes against a persisted
        100 must persist 0. Under a truthiness guard the 100 survives, a
        later 50-byte observation reads as no progress, and the stall
        clock never resets for a download that is moving.
        """
        state = self._state(files=[_seeded_file(
            bytes_transferred=100, last_state="InProgress")])

        result = self._reduce(
            state,
            self._snapshot(PollFileSnapshot(
                transfer_id="tx-1", state="InProgress",
                bytes_transferred=0)),
        )

        assert result.state is not None
        self.assertEqual(result.state.files[0].bytes_transferred, 0)
        self.assertEqual(
            result.verdict.decision, PollCycleDecision.in_progress)

    def test_a_change_into_a_non_progress_state_is_not_progress(self):
        """Must-still-work control for the case above.

        A file entering the peer's own queue has not progressed, so the
        stall clock must keep running from the last real progress.
        """
        state = self._state(
            enqueued_at="2026-07-11T02:50:00+00:00",
            last_progress_at="2026-07-11T02:50:00+00:00",
        )

        result = self._reduce(
            state,
            self._snapshot(PollFileSnapshot(
                transfer_id="tx-1", state="Queued, Remotely",
                bytes_transferred=0)),
        )

        assert result.state is not None
        self.assertEqual(
            result.state.last_progress_at, "2026-07-11T02:50:00+00:00")

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
