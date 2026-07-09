#!/usr/bin/env python3
"""Generated + pinned tests for issue #564 — sane download-failure evidence.

Three invariants, each shipped as a deterministic pin (already living in
``tests/test_download.py`` / ``tests/test_integration_slices.py`` /
``tests/test_enqueue_fanout.py``) AND a generated property here, per
``.claude/rules/code-quality.md`` § "Pin+fuzz PAIR rule":

I1. **No terminal observation is ever lost.** Any file whose transfer
    slskd reports in a terminal state (``"Completed, *"``) during a poll
    (``lib.download._capture_download_progress``) OR during the
    pre-purge harvest (``lib.download.harvest_terminal_transfer_evidence``)
    must have that state AND its exception persisted by the end of the
    cycle. Two sub-properties share this shape:
      - I1a — the poll-cycle capture/persistence-gate split (root cause
        #2: a transition INTO a terminal error state that wasn't
        "forward progress" was silently dropped).
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

Checkers are module-level functions with known-bad self-tests per the
house method (CLAUDE.md "Bug Hunting — Generated-First" /
code-quality.md Red/Green TDD). Profiles and promotion policy:
tests/_hypothesis_profiles.py and docs/generated-testing.md.
"""

import os
import sys
import unittest
from datetime import datetime, timezone
from typing import Any

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)

import msgspec
from hypothesis import example, given
from hypothesis import strategies as st

from lib.download import (
    _capture_download_progress,
    _enrich_timeout_reason,
    _vanished_timeout_reason,
    harvest_terminal_transfer_evidence,
    summarize_file_failures,
)
from lib.enqueue import _stamp_enqueue_failure_reason
from lib.quality import ActiveDownloadFileState, ActiveDownloadState
from lib.slskd_client import TransferSnapshot
from tests.fakes import FakePipelineDB, FakeSlskdAPI
from tests.helpers import make_ctx_with_fake_db, make_download_file, make_request_row

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
# I1a -- _capture_download_progress never loses a terminal observation
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
    state_dirty: bool,
) -> None:
    """Module-level checker (known-bad self-tests below)."""
    if not has_snapshot:
        if after_last_state != prev_state or after_last_exception != prev_exception:
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
    already_persisted = (
        prev_state == snap_state
        and prev_exception == expected_exception
        and prev_bytes == snap_bytes
    )
    if not already_persisted and not state_dirty:
        raise AssertionError(
            "terminal observation newly arrived this cycle but was not "
            "marked state_dirty -- it would never reach persistence "
            "(issue #564 root cause #2)")


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
    f = make_download_file(
        last_state=world["prev_state"], last_exception=world["prev_exception"],
        bytes_transferred=world["prev_bytes"])
    if world["has_snapshot"]:
        f.status = TransferSnapshot(
            state=world["snap_state"], bytes_transferred=world["snap_bytes"],
            exception=world["snap_exception"])
    state = ActiveDownloadState(
        filetype="flac", enqueued_at="2026-01-01T00:00:00+00:00", files=[])
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    _progress_made, state_dirty = _capture_download_progress([f], state, now)
    return dict(
        after_last_state=f.last_state, after_last_exception=f.last_exception,
        state_dirty=state_dirty,
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
            state_dirty=True,
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

    def test_trips_when_new_terminal_observation_not_marked_dirty(self):
        with self.assertRaises(AssertionError):
            assert_capture_progress_preserves_terminal_observation(
                **self._base(state_dirty=False))

    def test_trips_when_no_snapshot_but_file_mutated(self):
        with self.assertRaises(AssertionError):
            assert_capture_progress_preserves_terminal_observation(**self._base(
                has_snapshot=False, after_last_state="Completed, Rejected"))


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


if __name__ == "__main__":
    unittest.main()
