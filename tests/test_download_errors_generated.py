#!/usr/bin/env python3
"""Generated + pinned tests for issue #564 — sane download-failure evidence.

Three invariants, each shipped as a deterministic pin (already living in
``tests/test_download.py`` / ``tests/test_integration_slices.py`` /
``tests/test_enqueue_fanout.py``) AND a generated property here, per
``.claude/rules/code-quality.md`` § "Pin+fuzz PAIR rule":

I1. **No terminal observation is ever lost.** Any file whose transfer
    slskd reports in a terminal state (``"Completed, *"``) during a poll
    (``lib.quality.reduce_poll_cycle``) OR during the
    pre-purge harvest (``lib.download.harvest_terminal_transfer_evidence``)
    must have that state AND its exception persisted by the end of the
    cycle. Two sub-properties share this shape:
      - I1a — the complete poll-cycle state result (root cause #2: a
        transition INTO a terminal error state that wasn't "forward
        progress" was silently dropped).
      - I1b — the pre-purge harvest (root cause #3: transfers that
        complete/error within the same cycle they were enqueued, before
        any poll observes them, had their evidence destroyed by
        ``remove_completed_downloads()``).
I2. **Timeout messages are derived from evidence.** The composed
    ``error_message`` mentions every distinct failure reason present in
    the entry's per-file evidence, and claims "never observed" ONLY when
    no file has any evidence at all.
I3. **Enqueue-failure reasons propagate to the eventual timeout.** When
    ``lib.enqueue._stamp_enqueue_failure_reason`` stamps a captured
    enqueue-failure reason (root cause #4), it reaches both
    ``lib.download.summarize_file_failures`` and both timeout-message
    composers.

Also covers issue #820 — attempt-scoped transfer matching (a stale
prior-attempt terminal record for the same ``(username, filename)`` slskd
queue key must never shadow, nor silently suppress, the CURRENT attempt's
genuine transfer). Three more invariants, same PAIR discipline, prefixed
``#820`` to disambiguate from the #564 invariants above:

#820-I1. **Attempt-scoped binding.** ``match_transfer_for_attempt`` never
    returns a terminal candidate whose lifecycle predates the attempt's
    ``not_before`` boundary.
#820-I2. **No stale shadowing.** When at least one post-boundary
    candidate ("survivor") exists for the key, ``match_transfer_for_attempt``
    returns the highest-priority survivor — never ``None``, never the
    stale pre-boundary record.
#820-I3. **End-to-end.** A prior-attempt terminal ``Completed, Succeeded``
    record (pre-boundary) alongside the current attempt's own terminal
    state (post-boundary) — driven through the REAL
    ``harvest_terminal_transfer_evidence`` and ``reduce_poll_cycle`` —
    must never produce a ``complete`` decision unless the current
    attempt's own state genuinely is ``Completed, Succeeded``.

Checkers are module-level functions with known-bad self-tests per the
house method (CLAUDE.md "Bug Hunting — Generated-First" /
code-quality.md Red/Green TDD). Profiles and promotion policy:
tests/_hypothesis_profiles.py and docs/generated-testing.md.
"""

import os
import sys
import unittest
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from typing import Any

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)

import msgspec
from hypothesis import example, given
from hypothesis import strategies as st

from lib.download import (
    _enrich_timeout_reason,
    _vanished_timeout_reason,
    harvest_terminal_transfer_evidence,
    summarize_file_failures,
)
from lib.enqueue import _stamp_enqueue_failure_reason
from lib.quality import (
    ActiveDownloadFileState,
    ActiveDownloadState,
    PollCycleConfig,
    PollCycleDecision,
    PollCycleSnapshot,
    PollFileSnapshot,
    reduce_poll_cycle,
)
from lib.slskd_transfers import match_transfer_for_attempt
from tests.fakes import FakePipelineDB, FakeSlskdAPI
from tests.helpers import (
    make_ctx_with_fake_db,
    make_download_directory,
    make_download_file,
    make_download_user,
    make_request_row,
    make_transfer_snapshot,
)

_TERMINAL_STATES = (
    "Completed, Succeeded",
    "Completed, Cancelled",
    "Completed, TimedOut",
    "Completed, Errored",
    "Completed, Rejected",
    "Completed, Aborted",
)
_NON_TERMINAL_STATES = ("", "InProgress", "Queued, Remotely", "Initializing")
_ALL_STATES = _TERMINAL_STATES + _NON_TERMINAL_STATES
_EXCEPTIONS = (
    "Transfer rejected: Banned",
    "Read error: Connection reset by peer",
    "Inactivity timeout of 900000 milliseconds was reached",
    "Soulseek.DownloadEnqueueException: File not shared.",
)


# ============================================================================
# I1a -- reduce_poll_cycle never loses a terminal observation
# ============================================================================

def assert_capture_progress_preserves_terminal_observation(
    *,
    prev_state: str | None,
    prev_exception: str | None,
    prev_bytes: int,
    has_snapshot: bool,
    snap_state: str,
    snap_exception: str | None,
    snap_bytes: int,
    after_last_state: str | None,
    after_last_exception: str | None,
    after_bytes: int,
) -> None:
    """Module-level checker (known-bad self-tests below)."""
    if not has_snapshot:
        if (
            after_last_state != prev_state
            or after_last_exception != prev_exception
            or after_bytes != prev_bytes
        ):
            raise AssertionError(
                "no snapshot observed this cycle, but the file's "
                "persisted evidence was mutated anyway")
        return
    if not snap_state.startswith("Completed,"):
        return  # non-terminal observation -- I1 has no obligation here
    if after_last_state != snap_state:
        raise AssertionError(
            f"terminal state observation lost: snapshot={snap_state!r} "
            f"after={after_last_state!r}")
    expected_exception = snap_exception or prev_exception
    if after_last_exception != expected_exception:
        raise AssertionError(
            f"terminal exception observation lost: expected="
            f"{expected_exception!r} after={after_last_exception!r}")
    if after_bytes != snap_bytes:
        raise AssertionError(
            f"terminal byte observation lost: expected={snap_bytes} "
            f"after={after_bytes}")


@st.composite
def _capture_progress_worlds(draw: Any) -> dict:
    return dict(
        prev_state=draw(st.one_of(st.none(), st.sampled_from(_ALL_STATES))),
        prev_exception=draw(st.one_of(st.none(), st.sampled_from(_EXCEPTIONS))),
        prev_bytes=draw(st.integers(min_value=0, max_value=10_000_000)),
        has_snapshot=draw(st.booleans()),
        snap_state=draw(st.sampled_from(_ALL_STATES)),
        snap_exception=draw(st.one_of(st.none(), st.sampled_from(_EXCEPTIONS))),
        snap_bytes=draw(st.integers(min_value=0, max_value=10_000_000)),
    )


def _run_capture_progress(world: dict) -> dict:
    f = ActiveDownloadFileState(
        username="user",
        filename="Album\\01.flac",
        file_dir="Album",
        size=1,
        last_state=world["prev_state"],
        last_exception=world["prev_exception"],
        bytes_transferred=world["prev_bytes"],
    )
    state = ActiveDownloadState(
        filetype="flac",
        enqueued_at="2026-01-01T00:00:00+00:00",
        files=[f],
    )
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    snapshot = PollFileSnapshot(
        transfer_id="tx-1" if world["has_snapshot"] else None,
        state=world["snap_state"] if world["has_snapshot"] else None,
        bytes_transferred=(world["snap_bytes"] if world["has_snapshot"] else 0),
        exception=(world["snap_exception"] if world["has_snapshot"] else None),
    )
    result = reduce_poll_cycle(
        state,
        PollCycleSnapshot(files=[snapshot], completion_current_path="/canonical"),
        now,
        PollCycleConfig(
            remote_queue_timeout=10_000,
            stalled_timeout=10_000,
            max_file_retries=5,
        ),
    )
    assert result.state is not None
    after = result.state.files[0]
    return dict(
        after_last_state=after.last_state,
        after_last_exception=after.last_exception,
        after_bytes=after.bytes_transferred,
    )


class TestGeneratedCaptureProgressNeverLosesTerminalObservation(unittest.TestCase):
    @given(world=_capture_progress_worlds())
    @example(world=dict(
        prev_state="InProgress", prev_exception=None, prev_bytes=0,
        has_snapshot=True, snap_state="Completed, Rejected",
        snap_exception="Transfer rejected: Banned", snap_bytes=0,
    ))
    def test_capture_progress_never_loses_terminal_observation(self, world):
        result = _run_capture_progress(world)
        assert_capture_progress_preserves_terminal_observation(**world, **result)


class TestCaptureProgressCheckerTripsOnViolations(unittest.TestCase):
    def _base(self, **overrides: Any) -> dict:
        defaults = dict(
            prev_state=None, prev_exception=None, prev_bytes=0,
            has_snapshot=True, snap_state="Completed, Rejected",
            snap_exception="Transfer rejected: Banned", snap_bytes=0,
            after_last_state="Completed, Rejected",
            after_last_exception="Transfer rejected: Banned",
            after_bytes=0,
        )
        defaults.update(overrides)
        return defaults

    def test_trips_when_terminal_state_not_recorded(self):
        with self.assertRaises(AssertionError):
            assert_capture_progress_preserves_terminal_observation(
                **self._base(after_last_state="InProgress"))

    def test_trips_when_exception_dropped(self):
        with self.assertRaises(AssertionError):
            assert_capture_progress_preserves_terminal_observation(
                **self._base(after_last_exception=None))

    def test_trips_when_terminal_bytes_are_dropped(self):
        with self.assertRaises(AssertionError):
            assert_capture_progress_preserves_terminal_observation(
                **self._base(snap_bytes=10, after_bytes=0))

    def test_trips_when_no_snapshot_but_file_mutated(self):
        with self.assertRaises(AssertionError):
            assert_capture_progress_preserves_terminal_observation(**self._base(
                has_snapshot=False, after_last_state="Completed, Rejected"))


# ============================================================================
# Pure reducer inputs are never mutated across any poll phase
# ============================================================================

def assert_reducer_inputs_unchanged(
    *,
    state_before: object,
    state_after: object,
    snapshot_before: object,
    snapshot_after: object,
) -> None:
    """The reducer may construct outputs but must never mutate its inputs."""
    if state_after != state_before:
        raise AssertionError("reduce_poll_cycle mutated persisted_state")
    if snapshot_after != snapshot_before:
        raise AssertionError("reduce_poll_cycle mutated snapshot")


def _state_shape(state: ActiveDownloadState) -> object:
    return msgspec.to_builtins(state)


def _snapshot_shape(snapshot: PollCycleSnapshot) -> object:
    return asdict(snapshot)


_REDUCER_PHASES = (
    "import_gate",
    "processing_recovery",
    "processing_blocked",
    "fresh_vanished",
    "old_vanished",
    "progress",
    "retry",
    "completion",
)


@st.composite
def _reducer_purity_worlds(draw: Any) -> dict[str, Any]:
    return {
        "phase": draw(st.sampled_from(_REDUCER_PHASES)),
        "prev_bytes": draw(st.integers(min_value=0, max_value=1_000_000)),
        "retry_count": draw(st.integers(min_value=0, max_value=4)),
        "exception": draw(st.one_of(st.none(), st.sampled_from(_EXCEPTIONS))),
    }


def _run_reducer_purity(world: dict[str, Any]) -> None:
    now = datetime(2026, 1, 1, 0, 10, tzinfo=timezone.utc)
    file = ActiveDownloadFileState(
        username="user",
        filename="Album\\01.flac",
        file_dir="Album",
        size=1_000_000,
        retry_count=world["retry_count"],
        bytes_transferred=world["prev_bytes"],
        last_exception=world["exception"],
    )
    state = ActiveDownloadState(
        filetype="flac",
        enqueued_at="2026-01-01T00:00:00+00:00",
        last_progress_at="2026-01-01T00:09:30+00:00",
        files=[file],
    )
    phase = world["phase"]
    expected = PollCycleDecision.in_progress
    snapshot = PollCycleSnapshot(files=[PollFileSnapshot(
        transfer_id="tx-1",
        state="InProgress",
        bytes_transferred=world["prev_bytes"] + 1,
    )])

    if phase == "import_gate":
        snapshot = PollCycleSnapshot(
            active_import_job_id=17,
            active_import_job_status="running",
            processing_blocked_reason="multiple_populated_paths",
        )
        expected = PollCycleDecision.wait_import_job
    elif phase == "processing_recovery":
        state.processing_started_at = "2026-01-01T00:09:00+00:00"
        state.current_path = "/old"
        snapshot = PollCycleSnapshot(processing_current_path="/recovered")
        expected = PollCycleDecision.processing
    elif phase == "processing_blocked":
        state.processing_started_at = "2026-01-01T00:09:00+00:00"
        state.current_path = "/old"
        snapshot = PollCycleSnapshot(
            processing_blocked_reason="legacy_shared_only",
        )
        expected = PollCycleDecision.wait_processing_recovery
    elif phase == "fresh_vanished":
        state.enqueued_at = "2026-01-01T00:09:30+00:00"
        snapshot = PollCycleSnapshot(files=[PollFileSnapshot()])
        expected = PollCycleDecision.wait_fresh_vanished
    elif phase == "old_vanished":
        snapshot = PollCycleSnapshot(files=[PollFileSnapshot()])
        expected = PollCycleDecision.timeout_vanished
    elif phase == "retry":
        state.files.append(ActiveDownloadFileState(
            username="user",
            filename="Album\\02.flac",
            file_dir="Album",
            size=1_000_000,
            last_state="InProgress",
        ))
        snapshot = PollCycleSnapshot(files=[
            PollFileSnapshot(
                transfer_id="tx-1",
                state="Completed, Rejected",
                bytes_transferred=world["prev_bytes"],
                exception=world["exception"],
            ),
            PollFileSnapshot(
                transfer_id="tx-2",
                state="InProgress",
                bytes_transferred=1,
            ),
        ])
        expected = PollCycleDecision.retry_files
    elif phase == "completion":
        snapshot = PollCycleSnapshot(
            files=[PollFileSnapshot(
                transfer_id="tx-1",
                state="Completed, Succeeded",
                bytes_transferred=1_000_000,
            )],
            completion_current_path="/canonical",
        )
        expected = PollCycleDecision.complete

    state_before = _state_shape(state)
    snapshot_before = _snapshot_shape(snapshot)
    result = reduce_poll_cycle(
        state,
        snapshot,
        now,
        PollCycleConfig(
            remote_queue_timeout=10_000,
            stalled_timeout=10_000,
            max_file_retries=5,
        ),
    )
    assert result.verdict.decision == expected
    assert_reducer_inputs_unchanged(
        state_before=state_before,
        state_after=_state_shape(state),
        snapshot_before=snapshot_before,
        snapshot_after=_snapshot_shape(snapshot),
    )


class TestGeneratedReducerInputsArePure(unittest.TestCase):
    @given(world=_reducer_purity_worlds())
    def test_reduce_poll_cycle_never_mutates_inputs(self, world):
        _run_reducer_purity(world)


class TestReducerInputPurityCheckerTripsOnViolations(unittest.TestCase):
    def test_trips_when_state_is_mutated(self):
        with self.assertRaises(AssertionError):
            assert_reducer_inputs_unchanged(
                state_before={"files": [{"retry_count": 0}]},
                state_after={"files": [{"retry_count": 1}]},
                snapshot_before={"files": []},
                snapshot_after={"files": []},
            )

    def test_trips_when_snapshot_is_mutated(self):
        with self.assertRaises(AssertionError):
            assert_reducer_inputs_unchanged(
                state_before={"files": []},
                state_after={"files": []},
                snapshot_before={"files": [{"state": "InProgress"}]},
                snapshot_after={"files": [{"state": "Completed, Errored"}]},
            )


# ============================================================================
# I1b -- harvest_terminal_transfer_evidence never loses a terminal
# observation for an eligible row
# ============================================================================

def assert_harvest_preserves_terminal_observation(
    *,
    processing_started: bool,
    prev_state: str | None,
    prev_exception: str | None,
    prev_bytes: int,
    has_snapshot_match: bool,
    snap_state: str,
    snap_exception: str | None,
    snap_bytes: int,
    after_state: str | None,
    after_exception: str | None,
    after_bytes: int,
) -> None:
    """Module-level checker (known-bad self-tests below)."""
    already_terminal = bool(prev_state) and prev_state.startswith("Completed,")
    should_be_untouched = (
        processing_started
        or already_terminal
        or not has_snapshot_match
        or not snap_state.startswith("Completed,")
    )
    if should_be_untouched:
        if (after_state != prev_state or after_exception != prev_exception
                or after_bytes != prev_bytes):
            raise AssertionError(
                "harvest mutated a file it should have left alone "
                f"(processing_started={processing_started} "
                f"already_terminal={already_terminal} "
                f"has_snapshot_match={has_snapshot_match} "
                f"snap_state={snap_state!r})")
        return
    if after_state != snap_state:
        raise AssertionError(
            f"harvest lost a terminal observation: snapshot={snap_state!r} "
            f"after={after_state!r}")
    expected_exception = snap_exception or prev_exception
    if after_exception != expected_exception:
        raise AssertionError(
            f"harvest lost the exception: expected={expected_exception!r} "
            f"after={after_exception!r}")
    if after_bytes != snap_bytes:
        raise AssertionError(
            f"harvest lost the byte count: expected={snap_bytes} "
            f"after={after_bytes}")


@st.composite
def _harvest_worlds(draw: Any) -> dict:
    return dict(
        processing_started=draw(st.booleans()),
        prev_state=draw(st.one_of(st.none(), st.sampled_from(_ALL_STATES))),
        prev_exception=draw(st.one_of(st.none(), st.sampled_from(_EXCEPTIONS))),
        prev_bytes=draw(st.integers(min_value=0, max_value=10_000_000)),
        has_snapshot_match=draw(st.booleans()),
        snap_state=draw(st.sampled_from(_ALL_STATES)),
        snap_exception=draw(st.one_of(st.none(), st.sampled_from(_EXCEPTIONS))),
        snap_bytes=draw(st.integers(min_value=0, max_value=10_000_000)),
    )


_HARVEST_USERNAME = "peer1"
_HARVEST_FILENAME = "peer1\\Music\\01.flac"


def _run_harvest(world: dict) -> dict:
    file_state = ActiveDownloadFileState(
        username=_HARVEST_USERNAME, filename=_HARVEST_FILENAME,
        file_dir="peer1\\Music", size=1000,
        last_state=world["prev_state"], last_exception=world["prev_exception"],
        bytes_transferred=world["prev_bytes"],
    )
    state = ActiveDownloadState(
        filetype="flac", enqueued_at="2026-01-01T00:00:00+00:00",
        files=[file_state],
        processing_started_at=(
            "2026-01-01T00:00:00+00:00" if world["processing_started"] else None
        ),
    )
    row = make_request_row(
        id=1, status="downloading",
        active_download_state=msgspec.to_builtins(state))
    db = FakePipelineDB()
    db.seed_request(row)
    slskd = FakeSlskdAPI()
    if world["has_snapshot_match"]:
        slskd.add_transfer(
            username=_HARVEST_USERNAME, directory="peer1\\Music",
            filename=_HARVEST_FILENAME, id="tid-1",
            state=world["snap_state"], bytesTransferred=world["snap_bytes"],
            exception=world["snap_exception"],
        )
    ctx = make_ctx_with_fake_db(db, slskd=slskd)

    harvest_terminal_transfer_evidence(ctx)

    after = db.request(1)["active_download_state"]["files"][0]
    return dict(
        after_state=after.get("last_state"),
        after_exception=after.get("last_exception"),
        after_bytes=after.get("bytes_transferred", 0),
    )


class TestGeneratedHarvestNeverLosesTerminalObservation(unittest.TestCase):
    @given(world=_harvest_worlds())
    @example(world=dict(
        processing_started=False, prev_state=None, prev_exception=None,
        prev_bytes=0, has_snapshot_match=True,
        snap_state="Completed, Rejected",
        snap_exception="Transfer rejected: Banned", snap_bytes=0,
    ))
    @example(world=dict(
        processing_started=False, prev_state="Completed, Succeeded",
        prev_exception=None, prev_bytes=1000, has_snapshot_match=True,
        snap_state="Completed, Errored", snap_exception="ignored",
        snap_bytes=1000,
    ))
    def test_harvest_never_loses_terminal_observation(self, world):
        result = _run_harvest(world)
        assert_harvest_preserves_terminal_observation(**world, **result)


class TestHarvestCheckerTripsOnViolations(unittest.TestCase):
    def _base(self, **overrides: Any) -> dict:
        defaults = dict(
            processing_started=False, prev_state=None, prev_exception=None,
            prev_bytes=0, has_snapshot_match=True,
            snap_state="Completed, Rejected",
            snap_exception="Transfer rejected: Banned", snap_bytes=0,
            after_state="Completed, Rejected",
            after_exception="Transfer rejected: Banned", after_bytes=0,
        )
        defaults.update(overrides)
        return defaults

    def test_trips_when_terminal_observation_dropped(self):
        with self.assertRaises(AssertionError):
            assert_harvest_preserves_terminal_observation(
                **self._base(after_state=None, after_exception=None))

    def test_trips_when_already_terminal_row_is_overwritten(self):
        with self.assertRaises(AssertionError):
            assert_harvest_preserves_terminal_observation(**self._base(
                prev_state="Completed, Succeeded", prev_exception=None,
                prev_bytes=500, after_state="Completed, Rejected",
                after_exception="Transfer rejected: Banned", after_bytes=0,
            ))

    def test_trips_when_processing_started_row_is_touched(self):
        with self.assertRaises(AssertionError):
            assert_harvest_preserves_terminal_observation(**self._base(
                processing_started=True, prev_state=None, prev_exception=None,
                prev_bytes=0, after_state="Completed, Rejected",
                after_exception="Transfer rejected: Banned", after_bytes=0,
            ))

    def test_trips_when_bytes_not_updated(self):
        with self.assertRaises(AssertionError):
            assert_harvest_preserves_terminal_observation(
                **self._base(snap_bytes=999, after_bytes=0))


# ============================================================================
# I2 -- timeout messages are derived from evidence
# ============================================================================

def _file_evidence_reason(last_state: str | None,
                          last_exception: str | None) -> str | None:
    """Reference derivation (independent of summarize_file_failures's own
    code) of one file's expected contribution to the summary."""
    if last_exception:
        return last_exception
    if (last_state and last_state.startswith("Completed,")
            and last_state != "Completed, Succeeded"):
        return last_state
    return None


@st.composite
def _fail_file_specs(draw: Any) -> tuple[tuple[str | None, str | None], ...]:
    n = draw(st.integers(min_value=0, max_value=6))
    specs = []
    for _ in range(n):
        last_state = draw(st.one_of(st.none(), st.sampled_from(_ALL_STATES)))
        last_exception = draw(st.one_of(st.none(), st.sampled_from(_EXCEPTIONS)))
        specs.append((last_state, last_exception))
    return tuple(specs)


def _files_from_specs(
    specs: tuple[tuple[str | None, str | None], ...],
) -> list:
    return [
        make_download_file(last_state=s, last_exception=e) for s, e in specs
    ]


def assert_summary_mentions_every_distinct_reason(
    specs: tuple[tuple[str | None, str | None], ...],
    summary: str | None,
) -> None:
    """Module-level checker (known-bad self-tests below)."""
    expected_reasons = {
        _file_evidence_reason(s, e) for s, e in specs
        if _file_evidence_reason(s, e) is not None
    }
    if not expected_reasons:
        if summary is not None:
            raise AssertionError(
                f"no file carries evidence but summary is not None: {summary!r}")
        return
    if summary is None:
        raise AssertionError(
            f"evidence exists ({expected_reasons}) but summary is None")
    for reason in expected_reasons:
        if f"'{reason}'" not in summary:
            raise AssertionError(
                f"summary is missing distinct reason {reason!r}: {summary!r}")
    if "'Completed, Succeeded'" in summary:
        raise AssertionError(
            f"summary must never mention a succeeded state: {summary!r}")


class TestGeneratedSummaryMentionsEveryReason(unittest.TestCase):
    @given(specs=_fail_file_specs())
    @example(specs=(("Completed, Errored", None), ("Completed, Rejected", None)))
    @example(specs=((None, "Transfer rejected: Banned"),))
    @example(specs=(("Completed, Succeeded", None),))
    def test_summary_mentions_every_distinct_reason(self, specs):
        summary = summarize_file_failures(_files_from_specs(specs))
        assert_summary_mentions_every_distinct_reason(specs, summary)

    @given(specs=_fail_file_specs())
    def test_summary_is_order_independent(self, specs):
        """Deterministic ordering claim: shuffling the input files
        produces byte-identical output."""
        forward = summarize_file_failures(_files_from_specs(specs))
        backward = summarize_file_failures(_files_from_specs(tuple(reversed(specs))))
        self.assertEqual(forward, backward)


class TestSummaryCheckerTripsOnViolations(unittest.TestCase):
    def test_trips_when_reason_missing_from_summary(self):
        with self.assertRaises(AssertionError):
            assert_summary_mentions_every_distinct_reason(
                (("Completed, Errored", None),), "1× 'Completed, Rejected'")

    def test_trips_when_evidence_exists_but_summary_is_none(self):
        with self.assertRaises(AssertionError):
            assert_summary_mentions_every_distinct_reason(
                ((None, "Transfer rejected: Banned"),), None)

    def test_trips_when_no_evidence_but_summary_present(self):
        with self.assertRaises(AssertionError):
            assert_summary_mentions_every_distinct_reason(
                (("Completed, Succeeded", None),), "1× 'ghost'")

    def test_trips_on_succeeded_state_leaking_into_summary(self):
        with self.assertRaises(AssertionError):
            assert_summary_mentions_every_distinct_reason(
                (("Completed, Errored", None),),
                "1× 'Completed, Errored', 1× 'Completed, Succeeded'")


def assert_never_observed_phrase_matches_evidence(
    specs: tuple[tuple[str | None, str | None], ...],
    vanished_reason: str,
) -> None:
    """Module-level checker (known-bad self-tests below)."""
    has_evidence = any(
        _file_evidence_reason(s, e) is not None for s, e in specs)
    mentions_never_observed = "before any status was observed" in vanished_reason
    if has_evidence and mentions_never_observed:
        raise AssertionError(
            "vanished reason falsely claims nothing was observed: "
            f"{vanished_reason!r}")
    if not has_evidence and not mentions_never_observed:
        raise AssertionError(
            "vanished reason should claim nothing was observed but "
            f"doesn't: {vanished_reason!r}")


class TestGeneratedVanishedNeverObservedClaim(unittest.TestCase):
    @given(specs=_fail_file_specs())
    @example(specs=())
    @example(specs=(("Completed, Errored", None),))
    def test_never_observed_claim_matches_evidence(self, specs):
        reason = _vanished_timeout_reason(_files_from_specs(specs))
        assert_never_observed_phrase_matches_evidence(specs, reason)


class TestVanishedClaimCheckerTripsOnViolations(unittest.TestCase):
    def test_trips_when_evidence_exists_but_claims_never_observed(self):
        with self.assertRaises(AssertionError):
            assert_never_observed_phrase_matches_evidence(
                (("Completed, Errored", None),),
                "transfers vanished from slskd before any status was "
                "observed (slskd restart?)")

    def test_trips_when_no_evidence_but_does_not_claim_never_observed(self):
        with self.assertRaises(AssertionError):
            assert_never_observed_phrase_matches_evidence(
                (), "transfers no longer in slskd")


# ============================================================================
# I3 -- enqueue-failure reasons propagate to the eventual timeout
# ============================================================================

def assert_stamped_reason_propagates(
    reason: str | None,
    summary: str | None,
    enriched: str,
    vanished: str,
) -> None:
    """Module-level checker (known-bad self-tests below)."""
    if not reason:
        if summary is not None:
            raise AssertionError(
                f"no reason stamped but summary is not None: {summary!r}")
        return
    fragment = f"enqueue failed: {reason}"
    if summary is None or fragment not in summary:
        raise AssertionError(
            f"stamped reason lost from summary: reason={reason!r} "
            f"summary={summary!r}")
    if fragment not in enriched:
        raise AssertionError(
            f"stamped reason lost from the enriched timeout reason: "
            f"{enriched!r}")
    if fragment not in vanished:
        raise AssertionError(
            f"stamped reason lost from the vanished-timeout reason: "
            f"{vanished!r}")


@st.composite
def _stamp_reason_worlds(draw: Any) -> tuple[str | None, int]:
    reason = draw(st.one_of(st.none(), st.text(min_size=1, max_size=80)))
    n = draw(st.integers(min_value=1, max_value=4))
    return reason, n


class TestGeneratedStampedReasonPropagates(unittest.TestCase):
    @given(world=_stamp_reason_worlds())
    @example(world=("Soulseek.DownloadEnqueueException: File not shared.", 2))
    @example(world=(None, 1))
    def test_stamped_reason_propagates_into_every_message(self, world):
        reason, n = world
        files = [make_download_file() for _ in range(n)]

        _stamp_enqueue_failure_reason(files, reason)

        summary = summarize_file_failures(files)
        enriched = _enrich_timeout_reason("some decision reason", files)
        vanished = _vanished_timeout_reason(files)
        assert_stamped_reason_propagates(reason, summary, enriched, vanished)


class TestStampedReasonCheckerTripsOnViolations(unittest.TestCase):
    def test_trips_when_reason_missing_from_summary(self):
        with self.assertRaises(AssertionError):
            assert_stamped_reason_propagates(
                "File not shared.", None,
                "some reason — enqueue failed: File not shared.",
                "vanished reason — enqueue failed: File not shared.")

    def test_trips_when_reason_missing_from_enriched(self):
        with self.assertRaises(AssertionError):
            assert_stamped_reason_propagates(
                "File not shared.",
                "1× 'enqueue failed: File not shared.'",
                "some reason without it",
                "vanished reason — enqueue failed: File not shared.")

    def test_trips_when_reason_missing_from_vanished(self):
        with self.assertRaises(AssertionError):
            assert_stamped_reason_propagates(
                "File not shared.",
                "1× 'enqueue failed: File not shared.'",
                "some reason — enqueue failed: File not shared.",
                "vanished reason without it")

    def test_trips_when_summary_present_but_no_reason_stamped(self):
        with self.assertRaises(AssertionError):
            assert_stamped_reason_propagates(
                None, "1× 'some ghost reason'", "irrelevant", "irrelevant")


# ============================================================================
# Issue #820 -- attempt-scoped transfer matching
# ============================================================================
#
# A stale prior-attempt terminal record for the SAME (username, filename)
# slskd queue key (visible forever via includeRemoved=True) must never
# shadow, nor silently suppress via a bare None return, the CURRENT
# attempt's own genuine transfer.

_820_USERNAME = "peer-820"
_820_DIRECTORY = "peer-820\\Album"
_820_FILENAME = "peer-820\\Album\\01.flac"
_820_BOUNDARY = datetime(2026, 7, 22, 2, 1, 25, tzinfo=timezone.utc)


def _820_is_survivor(spec: tuple[str, int]) -> bool:
    """A candidate survives attempt-boundary filtering unless it is BOTH
    terminal AND pre-boundary -- mirrors ``_is_terminal_transfer_before``:
    a non-terminal (in-progress/queued) candidate always survives,
    whatever its own timestamps."""
    state, offset_seconds = spec
    return not (state.startswith("Completed,") and offset_seconds < 0)


def _820_reference_priority(state: str, offset_seconds: int) -> tuple[int, int, int]:
    """Independent re-derivation of ``_transfer_priority``'s ranking
    tuple, over the same (state, offset) shape the strategy draws --
    deliberately NOT calling the production function, so the property
    doesn't just check the implementation against itself."""
    is_terminal = state.startswith("Completed,")
    is_success = state == "Completed, Succeeded"
    return (0 if is_terminal else 1, 1 if is_success else 0, offset_seconds)


def _820_build_downloads(specs: tuple[tuple[str, int], ...]):
    files = [
        make_transfer_snapshot(
            filename=_820_FILENAME,
            id=f"c{i}",
            state=state,
            ended_at=(_820_BOUNDARY + timedelta(seconds=offset)).isoformat(),
        )
        for i, (state, offset) in enumerate(specs)
    ]
    return make_download_user(
        username=_820_USERNAME,
        directories=[make_download_directory(
            directory=_820_DIRECTORY, files=files)],
    )


def _820_run_boundary_world(specs: tuple[tuple[str, int], ...]) -> int | None:
    downloads = _820_build_downloads(specs)
    result = match_transfer_for_attempt(
        downloads, _820_FILENAME, username=_820_USERNAME,
        not_before=_820_BOUNDARY.isoformat(),
    )
    if result is None:
        return None
    return int(result.id[1:])


@st.composite
def _820_boundary_worlds(draw: st.DrawFn) -> tuple[tuple[str, int], ...]:
    n = draw(st.integers(min_value=1, max_value=5))
    specs = []
    for _ in range(n):
        state = draw(st.sampled_from(_ALL_STATES))
        offset = draw(st.integers(min_value=-90 * 86400, max_value=90 * 86400))
        specs.append((state, offset))
    return tuple(specs)


# ---- #820-I1: attempt-scoped binding --------------------------------------

def assert_never_returns_pre_boundary_terminal(
    *,
    specs: tuple[tuple[str, int], ...],
    result_index: int | None,
) -> None:
    """Module-level checker (known-bad self-tests below)."""
    if result_index is None:
        return
    if not _820_is_survivor(specs[result_index]):
        state, offset = specs[result_index]
        raise AssertionError(
            f"matcher returned a pre-boundary terminal candidate: "
            f"state={state!r} offset={offset}s — attempt boundary violated")


class TestGeneratedMatchNeverReturnsPreBoundaryTerminal(unittest.TestCase):
    @given(specs=_820_boundary_worlds())
    @example(specs=(
        ("Completed, Succeeded", -65 * 86400),
        ("Completed, Errored", 1),
    ))
    def test_match_never_returns_pre_boundary_terminal(self, specs):
        result_index = _820_run_boundary_world(specs)
        assert_never_returns_pre_boundary_terminal(
            specs=specs, result_index=result_index)


class TestPreBoundaryCheckerTripsOnViolations(unittest.TestCase):
    def test_trips_when_pre_boundary_terminal_is_returned(self):
        with self.assertRaises(AssertionError):
            assert_never_returns_pre_boundary_terminal(
                specs=(("Completed, Succeeded", -10),), result_index=0)

    def test_does_not_trip_on_none(self):
        assert_never_returns_pre_boundary_terminal(
            specs=(("Completed, Succeeded", -10),), result_index=None)

    def test_does_not_trip_on_post_boundary_result(self):
        assert_never_returns_pre_boundary_terminal(
            specs=(("Completed, Errored", 10),), result_index=0)


# ---- #820-I2: no stale shadowing -------------------------------------------

def assert_returns_best_survivor_when_any_exist(
    *,
    specs: tuple[tuple[str, int], ...],
    result_index: int | None,
) -> None:
    """Module-level checker (known-bad self-tests below)."""
    survivor_indices = [i for i, s in enumerate(specs) if _820_is_survivor(s)]
    if not survivor_indices:
        if result_index is not None:
            raise AssertionError(
                "matcher returned a result despite zero survivors")
        return
    if result_index is None:
        raise AssertionError(
            f"matcher returned None despite {len(survivor_indices)} "
            f"survivor(s): {[specs[i] for i in survivor_indices]}")
    if result_index not in survivor_indices:
        raise AssertionError(
            f"matcher returned a non-survivor: {specs[result_index]}")
    best = max(
        survivor_indices, key=lambda i: _820_reference_priority(*specs[i]))
    if (_820_reference_priority(*specs[result_index])
            != _820_reference_priority(*specs[best])):
        raise AssertionError(
            "matcher did not return the highest-priority survivor: "
            f"returned={specs[result_index]} best={specs[best]}")


class TestGeneratedMatchReturnsBestSurvivor(unittest.TestCase):
    @given(specs=_820_boundary_worlds())
    @example(specs=(
        ("Completed, Succeeded", -65 * 86400),
        ("Completed, Errored", 1),
    ))
    def test_match_returns_best_survivor(self, specs):
        result_index = _820_run_boundary_world(specs)
        assert_returns_best_survivor_when_any_exist(
            specs=specs, result_index=result_index)


class TestBestSurvivorCheckerTripsOnViolations(unittest.TestCase):
    def test_trips_when_none_returned_despite_survivor(self):
        with self.assertRaises(AssertionError):
            assert_returns_best_survivor_when_any_exist(
                specs=(("Completed, Errored", 5),), result_index=None)

    def test_trips_when_non_survivor_returned(self):
        with self.assertRaises(AssertionError):
            assert_returns_best_survivor_when_any_exist(
                specs=(
                    ("Completed, Succeeded", -5),
                    ("Completed, Errored", 5),
                ),
                result_index=0)

    def test_trips_when_lower_priority_survivor_returned(self):
        with self.assertRaises(AssertionError):
            assert_returns_best_survivor_when_any_exist(
                specs=(
                    ("Completed, Errored", 5),
                    ("Completed, Succeeded", 10),
                ),
                result_index=0)

    def test_does_not_trip_when_no_survivors_and_none_returned(self):
        assert_returns_best_survivor_when_any_exist(
            specs=(("Completed, Succeeded", -5),), result_index=None)


# ---- #820-I3: end-to-end — no false 'complete' -----------------------------

_820_STALE_ENDED_AT = "2026-05-18T23:04:58+00:00"  # real ~65-day May gap


def _820_run_stale_shadow_world(world: dict) -> PollCycleDecision:
    """Drive the REAL harvest, matcher, and reducer together over a
    two-record world: a prior-attempt terminal Succeeded record fixed at
    the real May gap (pre-boundary), alongside a current-attempt record
    at the drawn state/offset (post-boundary)."""
    row = make_request_row(
        id=1, status="downloading",
        active_download_state={
            "filetype": "flac",
            "enqueued_at": _820_BOUNDARY.isoformat(),
            "files": [{
                "username": _820_USERNAME, "filename": _820_FILENAME,
                "file_dir": _820_DIRECTORY, "size": 1000,
            }],
        },
    )
    db = FakePipelineDB()
    db.seed_request(row)
    slskd = FakeSlskdAPI()
    slskd.add_transfer(
        username=_820_USERNAME, directory=_820_DIRECTORY,
        filename=_820_FILENAME, id="stale",
        state="Completed, Succeeded", endedAt=_820_STALE_ENDED_AT,
    )
    current_ended_at = (
        _820_BOUNDARY + timedelta(seconds=world["post_offset"])
    ).isoformat()
    slskd.add_transfer(
        username=_820_USERNAME, directory=_820_DIRECTORY,
        filename=_820_FILENAME, id="current",
        state=world["current_state"], endedAt=current_ended_at,
        exception=world["exception"],
    )
    ctx = make_ctx_with_fake_db(db, slskd=slskd)

    # Seam 1: end-of-cycle harvest.
    harvest_terminal_transfer_evidence(ctx)

    # Seam 2: the next poll's own matcher + reducer, over whatever
    # harvest just persisted.
    state = ActiveDownloadState.from_raw(
        db.request(1)["active_download_state"])
    transfer = match_transfer_for_attempt(
        slskd.transfers.get_all_downloads(includeRemoved=True),
        _820_FILENAME, username=_820_USERNAME,
        not_before=_820_BOUNDARY.isoformat(),
    )
    snapshot = PollFileSnapshot(
        transfer_id=transfer.id if transfer is not None else None,
        state=transfer.state if transfer is not None else None,
        bytes_transferred=(
            transfer.bytes_transferred if transfer is not None else 0),
        exception=transfer.exception if transfer is not None else None,
    )
    now = _820_BOUNDARY + timedelta(seconds=max(world["post_offset"], 0) + 60)
    result = reduce_poll_cycle(
        state,
        PollCycleSnapshot(files=[snapshot], completion_current_path="/canon"),
        now,
        PollCycleConfig(
            remote_queue_timeout=1_000_000, stalled_timeout=1_000_000,
            max_file_retries=5),
    )
    return result.verdict.decision


def assert_stale_shadow_never_produces_false_complete(
    *,
    current_state: str,
    decision: PollCycleDecision,
) -> None:
    """Module-level checker (known-bad self-tests below)."""
    if (
        current_state != "Completed, Succeeded"
        and decision == PollCycleDecision.complete
    ):
        raise AssertionError(
            f"stale prior-attempt record laundered current_state="
            f"{current_state!r} into decision=PollCycleDecision.complete")


@st.composite
def _820_stale_shadow_worlds(draw: st.DrawFn) -> dict:
    return dict(
        current_state=draw(st.sampled_from(_ALL_STATES)),
        post_offset=draw(st.integers(min_value=0, max_value=7200)),
        exception=draw(st.one_of(st.none(), st.sampled_from(_EXCEPTIONS))),
    )


class TestGeneratedStaleShadowNeverProducesFalseComplete(unittest.TestCase):
    @given(world=_820_stale_shadow_worlds())
    @example(world=dict(
        current_state="Completed, Errored", post_offset=1,
        exception=(
            "Download of 09 - Downhill From Here.mp3 reported as "
            "failed by HumDrum"),
    ))
    def test_stale_shadow_never_produces_false_complete(self, world):
        decision = _820_run_stale_shadow_world(world)
        assert_stale_shadow_never_produces_false_complete(
            current_state=world["current_state"], decision=decision)


class TestStaleShadowCheckerTripsOnViolations(unittest.TestCase):
    def test_trips_when_errored_state_produces_complete(self):
        with self.assertRaises(AssertionError):
            assert_stale_shadow_never_produces_false_complete(
                current_state="Completed, Errored",
                decision=PollCycleDecision.complete)

    def test_does_not_trip_when_genuinely_succeeded_produces_complete(self):
        assert_stale_shadow_never_produces_false_complete(
            current_state="Completed, Succeeded",
            decision=PollCycleDecision.complete)

    def test_does_not_trip_when_non_complete_decision(self):
        assert_stale_shadow_never_produces_false_complete(
            current_state="Completed, Errored",
            decision=PollCycleDecision.retry_files)


if __name__ == "__main__":
    unittest.main()
