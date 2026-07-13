#!/usr/bin/env python3
"""Generated invariants for owned terminal slskd transfer convergence.

Generated worlds vary ledger ownership/stamp state and live slskd state.
The real purge entry point must preserve foreign and nonterminal records,
keep unstamped successes behind the authoritative completion-event/local-
path gate, terminal-stamp every owned non-success ``Completed,*`` outcome,
and remove only terminal records whose durable stamp is confirmed.

The deterministic pins for these same invariants live in
``tests/test_download.py::TestPurgeCompletedTransfers`` (orchestration)
and ``tests/test_repair.py::TestFindCompletedTransfersToPurge`` (pure
classification).

Profiles and promotion policy: tests/_hypothesis_profiles.py and
docs/generated-testing.md.
"""
from __future__ import annotations

import configparser
import os
import sys
import unittest
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)

from hypothesis import given
from hypothesis import strategies as st

from lib.config import CratediggerConfig
from lib.context import CratediggerContext
from lib.pipeline_db import TerminalFailureClaim, TransferLedgerRow
from lib.slskd_transfers import purge_completed_transfers
from tests.fakes import FakePipelineDB, FakePipelineDBSource, FakeSlskdAPI

_LIVE_STATES = ("InProgress", "Queued, Remotely", "Queued, Locally", "")
_TERMINAL_STATES = (
    "Completed, Succeeded",
    "Completed, Errored",
    "Completed, Cancelled",
    "Completed, TimedOut",
    "Completed, Aborted",
    "Completed, Rejected",
)
_ALL_STATES = _LIVE_STATES + _TERMINAL_STATES

_OWNERSHIPS = (
    "stamped", "failure_stamped", "unstamped", "unbound", "foreign")


@dataclass(frozen=True)
class CompletedTransferWorld:
    key: int
    state: str
    ownership: str


@st.composite
def completed_transfer_worlds(draw) -> tuple[CompletedTransferWorld, ...]:
    count = draw(st.integers(min_value=0, max_value=6))
    worlds = []
    for i in range(count):
        worlds.append(CompletedTransferWorld(
            key=i,
            state=draw(st.sampled_from(_ALL_STATES)),
            ownership=draw(st.sampled_from(_OWNERSHIPS)),
        ))
    return tuple(worlds)


def _username(key: int) -> str:
    return f"peer{key}"


def _filename(key: int) -> str:
    return f"Music\\Album{key}\\track.flac"


def _cfg() -> CratediggerConfig:
    return CratediggerConfig.from_ini(configparser.ConfigParser())


def _build_world_fakes(
    worlds: tuple[CompletedTransferWorld, ...],
) -> tuple[FakePipelineDB, FakeSlskdAPI]:
    db = FakePipelineDB()
    slskd = FakeSlskdAPI()
    for w in worlds:
        username, filename = _username(w.key), _filename(w.key)
        transfer_id = f"t-{w.key}"
        slskd.add_transfer(
            username=username, directory=f"Music\\Album{w.key}",
            filename=filename, id=transfer_id, state=w.state,
            requestedAt=(datetime.now(timezone.utc) + timedelta(seconds=1)).isoformat())
        if w.ownership in (
            "stamped", "failure_stamped", "unstamped", "unbound",
        ):
            db.record_transfer_enqueue([
                TransferLedgerRow(
                    request_id=w.key + 1, username=username,
                    filename=filename),
            ])
            if w.ownership == "stamped":
                db.stamp_transfer_completion(
                    username, filename, "/downloads/complete/x",
                    datetime.now(timezone.utc),
                    transfer_id=transfer_id)
            elif w.ownership == "failure_stamped":
                db.stamp_transfer_id(username, filename, transfer_id)
                db.stamp_terminal_failures(
                    {transfer_id}, datetime.now(timezone.utc))
            elif w.ownership == "unstamped":
                db.stamp_transfer_id(username, filename, transfer_id)
        # "foreign" -- no ledger row at all.
    return db, slskd


def _ctx(db: FakePipelineDB, slskd: FakeSlskdAPI) -> CratediggerContext:
    return CratediggerContext(
        cfg=_cfg(), slskd=slskd, pipeline_db_source=FakePipelineDBSource(db))


# --- Invariant checkers (module-level so the known-bad self-tests can
# call them directly) --------------------------------------------------


def assert_foreign_never_mutated(
    worlds: tuple[CompletedTransferWorld, ...],
    stamped_ids: set[str],
    removed_ids: set[str],
) -> None:
    """A transfer absent from the ledger is never stamped or removed."""
    for w in worlds:
        if w.ownership != "foreign":
            continue
        transfer_id = f"t-{w.key}"
        if transfer_id in stamped_ids or transfer_id in removed_ids:
            raise AssertionError(
                f"foreign transfer {transfer_id!r} (world={w!r}) was mutated")


def assert_nonterminal_never_mutated(
    worlds: tuple[CompletedTransferWorld, ...],
    newly_stamped_ids: set[str],
    removed_ids: set[str],
) -> None:
    """A nonterminal transfer is never newly stamped or removed."""
    for w in worlds:
        if w.state.startswith("Completed,"):
            continue
        transfer_id = f"t-{w.key}"
        if transfer_id in newly_stamped_ids or transfer_id in removed_ids:
            raise AssertionError(
                f"nonterminal transfer {transfer_id!r} (world={w!r}) was mutated")


def assert_unstamped_success_waits_for_event(
    worlds: tuple[CompletedTransferWorld, ...],
    newly_stamped_ids: set[str],
    removed_ids: set[str],
) -> None:
    """An unstamped success awaits the authoritative event/local path."""
    for w in worlds:
        if not (
            w.ownership in ("failure_stamped", "unstamped", "unbound")
            and w.state == "Completed, Succeeded"
        ):
            continue
        transfer_id = f"t-{w.key}"
        if transfer_id in newly_stamped_ids or transfer_id in removed_ids:
            raise AssertionError(
                f"unstamped success {transfer_id!r} (world={w!r}) bypassed "
                "the completion-event gate")


def assert_owned_terminal_failure_is_stamped_and_removed(
    worlds: tuple[CompletedTransferWorld, ...],
    newly_stamped_ids: set[str],
    removed_ids: set[str],
) -> None:
    """Every unstamped owned terminal failure is stamped then removed."""
    for w in worlds:
        if not (
            w.ownership in ("unstamped", "unbound")
            and w.state.startswith("Completed,")
            and w.state != "Completed, Succeeded"
        ):
            continue
        transfer_id = f"t-{w.key}"
        if transfer_id not in newly_stamped_ids or transfer_id not in removed_ids:
            raise AssertionError(
                f"owned terminal failure {transfer_id!r} (world={w!r}) did "
                "not converge through stamp then removal")


def assert_stamped_owned_completed_is_removed(
    worlds: tuple[CompletedTransferWorld, ...], removed_ids: set[str],
) -> None:
    """P3: a stamped, ledger-owned, COMPLETED transfer IS removed."""
    for w in worlds:
        if not w.state.startswith("Completed"):
            continue
        if w.ownership == "stamped":
            pass
        elif (
            w.ownership == "failure_stamped"
            and w.state != "Completed, Succeeded"
        ):
            pass
        else:
            continue
        transfer_id = f"t-{w.key}"
        if transfer_id not in removed_ids:
            raise AssertionError(
                f"stamped, owned, completed transfer {transfer_id!r} "
                f"(world={w!r}) was NOT removed by the purge")


def assert_one_to_one_claiming(
    ledger_count: int,
    failure_count: int,
    claimed_ids: set[str],
    removed_ids: set[str],
) -> None:
    """Unbound failures consume no more than one causal T1 row each."""
    expected_count = min(ledger_count, failure_count)
    if len(claimed_ids) != expected_count or removed_ids != claimed_ids:
        raise AssertionError(
            "unbound failure claiming was not one-to-one: "
            f"ledger={ledger_count} failures={failure_count} "
            f"claimed={claimed_ids!r} removed={removed_ids!r}")


def assert_failed_stamp_write_fails_closed(
    transfer_id: str,
    newly_stamped_ids: set[str],
    removed_ids: set[str],
) -> None:
    """A persistence failure cannot authorize terminal removal."""
    if transfer_id in newly_stamped_ids or transfer_id in removed_ids:
        raise AssertionError(
            f"failed persistence still mutated {transfer_id!r}")


def assert_failure_stamp_success_gate(
    removed_before_event: set[str],
    removed_after_event: set[str],
    transfer_id: str,
) -> None:
    """A pathless failure stamp cannot authorize a live success."""
    if transfer_id in removed_before_event or transfer_id not in removed_after_event:
        raise AssertionError(
            f"failure-stamped success gate violated for {transfer_id!r}")


def assert_terminal_summary_is_disjoint(
    worlds: tuple[CompletedTransferWorld, ...],
    *,
    removed: int,
    success_waiting: int,
    failure_unconfirmed: int,
    foreign: int,
) -> None:
    terminal_count = sum(
        world.state.startswith("Completed,") for world in worlds)
    accounted = removed + success_waiting + failure_unconfirmed + foreign
    if accounted != terminal_count:
        raise AssertionError(
            f"terminal accounting overlaps or omits rows: "
            f"expected={terminal_count} accounted={accounted}")


def assert_transfer_id_claim_is_globally_unique(
    transfer_id: str,
    ledger_transfer_ids: list[str | None],
) -> None:
    if ledger_transfer_ids.count(transfer_id) > 1:
        raise AssertionError(
            f"transfer ID {transfer_id!r} was claimed by multiple rows")


class TestGeneratedPurgeCompletedTransfers(unittest.TestCase):
    """P1 + P2 + P3 properties over generated worlds, through the REAL
    ``purge_completed_transfers`` entry point."""

    @given(worlds=completed_transfer_worlds())
    def test_terminal_convergence_invariants_hold_across_worlds(self, worlds):
        db, slskd = _build_world_fakes(worlds)
        before_stamped = {
            row.transfer_id for row in db._transfer_ledger.values()
            if row.transfer_id is not None and row.completed_at is not None
        }

        summary = purge_completed_transfers(_ctx(db, slskd))

        stamped_ids = {
            row.transfer_id for row in db._transfer_ledger.values()
            if row.transfer_id is not None and row.completed_at is not None
        }
        newly_stamped_ids = stamped_ids - before_stamped
        removed_ids = {
            c.id for c in slskd.transfers.cancel_download_calls if c.remove
        }
        assert_foreign_never_mutated(worlds, stamped_ids, removed_ids)
        assert_nonterminal_never_mutated(
            worlds, newly_stamped_ids, removed_ids)
        assert_unstamped_success_waits_for_event(
            worlds, newly_stamped_ids, removed_ids)
        assert_owned_terminal_failure_is_stamped_and_removed(
            worlds, newly_stamped_ids, removed_ids)
        assert_stamped_owned_completed_is_removed(worlds, removed_ids)
        assert_terminal_summary_is_disjoint(
            worlds,
            removed=summary.removed,
            success_waiting=summary.success_waiting,
            failure_unconfirmed=summary.failure_unconfirmed,
            foreign=summary.foreign_count,
        )

    @given(worlds=completed_transfer_worlds())
    def test_purge_is_idempotent_second_pass_removes_nothing_new(
        self, worlds,
    ):
        """A second pass over the same (now-purged) slskd state finds no
        NEW removable records — the first pass's removals already took
        them out of the live snapshot (FakeSlskdAPI mirrors slskd's own
        remove-on-cancel behavior)."""
        db, slskd = _build_world_fakes(worlds)
        purge_completed_transfers(_ctx(db, slskd))
        first_pass_calls = list(slskd.transfers.cancel_download_calls)

        purge_completed_transfers(_ctx(db, slskd))

        self.assertEqual(slskd.transfers.cancel_download_calls, first_pass_calls)

    @given(worlds=completed_transfer_worlds())
    def test_every_removal_call_passes_remove_true(self, worlds):
        """Seam guard: the purge must always request record removal, not
        a bare cancel — a plain cancel_download(..., remove=False) on an
        already-completed transfer is a no-op against slskd, silently
        leaving the record in place."""
        db, slskd = _build_world_fakes(worlds)

        purge_completed_transfers(_ctx(db, slskd))

        for call in slskd.transfers.cancel_download_calls:
            self.assertTrue(
                call.remove, f"cancel_download call {call!r} must set remove=True")

    @given(
        ledger_count=st.integers(min_value=0, max_value=5),
        failure_count=st.integers(min_value=0, max_value=5),
    )
    def test_unbound_duplicate_keys_claim_at_most_one_row_per_failure(
        self, ledger_count, failure_count,
    ):
        """Unknown terminal IDs consume causal T1 rows one-to-one."""
        db = FakePipelineDB()
        slskd = FakeSlskdAPI()
        base = datetime(2026, 7, 13, tzinfo=timezone.utc)
        for index in range(ledger_count):
            db.record_transfer_enqueue([
                TransferLedgerRow(
                    request_id=index + 1, username="peer",
                    filename="Music\\Retry\\track.flac"),
            ])
            newest = max(db._transfer_ledger.values(), key=lambda row: row.id)
            newest.enqueued_at = base + timedelta(minutes=index)
        for index in range(failure_count):
            slskd.add_transfer(
                username="peer", directory="Music\\Retry",
                filename="Music\\Retry\\track.flac", id=f"failure-{index}",
                state="Completed, Errored",
                requestedAt=(base + timedelta(minutes=index, seconds=30)).isoformat(),
            )

        purge_completed_transfers(_ctx(db, slskd))

        claimed = {
            row.transfer_id for row in db._transfer_ledger.values()
            if row.transfer_id is not None
        }
        removed = {
            call.id for call in slskd.transfers.cancel_download_calls
            if call.remove
        }
        assert_one_to_one_claiming(
            ledger_count, failure_count, claimed, removed)

    @given(
        ownership=st.sampled_from(("unstamped", "unbound")),
        state=st.sampled_from(_TERMINAL_STATES[1:]),
    )
    def test_terminal_stamp_write_failures_are_fail_closed(
        self, ownership, state,
    ):
        worlds = (CompletedTransferWorld(
            key=0, state=state, ownership=ownership),)
        db, slskd = _build_world_fakes(worlds)
        if ownership == "unstamped":
            db.set_stamp_terminal_failures_error(RuntimeError("write failed"))
        else:
            db.set_claim_terminal_failures_error(RuntimeError("write failed"))

        purge_completed_transfers(_ctx(db, slskd))

        newly_stamped = {
            row.transfer_id for row in db._transfer_ledger.values()
            if row.transfer_id is not None and row.completed_at is not None
        }
        removed = {
            call.id for call in slskd.transfers.cancel_download_calls
            if call.remove
        }
        assert_failed_stamp_write_fails_closed(
            "t-0", newly_stamped, removed)

    @given(
        has_causal_timestamp=st.booleans(),
        gap_minutes=st.integers(min_value=0, max_value=10),
    )
    def test_unbound_claim_requires_a_recent_causal_timestamp(
        self, has_causal_timestamp, gap_minutes,
    ):
        db = FakePipelineDB()
        slskd = FakeSlskdAPI()
        requested_at = datetime(2026, 7, 13, tzinfo=timezone.utc)
        db.record_transfer_enqueue([
            TransferLedgerRow(
                request_id=1, username="peer", filename="Music\\a.flac"),
        ])
        row = next(iter(db._transfer_ledger.values()))
        row.enqueued_at = requested_at - timedelta(minutes=gap_minutes)
        slskd.add_transfer(
            username="peer", directory="Music", filename="Music\\a.flac",
            id="failure", state="Completed, Errored",
            requestedAt=(
                requested_at.isoformat()
                if has_causal_timestamp else None
            ),
        )

        purge_completed_transfers(_ctx(db, slskd))

        removed = {
            call.id for call in slskd.transfers.cancel_download_calls
            if call.remove
        }
        expected = (
            {"failure"}
            if has_causal_timestamp and gap_minutes <= 5
            else set()
        )
        self.assertEqual(removed, expected)

    @given(state=st.sampled_from(_TERMINAL_STATES[1:]))
    def test_failure_stamp_then_success_requires_event_path(self, state):
        db = FakePipelineDB()
        filename = "Music\\Retry\\track.flac"
        db.record_transfer_enqueue([
            TransferLedgerRow(
                request_id=1, username="peer", filename=filename),
        ])
        db.stamp_transfer_id("peer", filename, "same-id")
        db.stamp_terminal_failures(
            {"same-id"}, datetime.now(timezone.utc))
        slskd = FakeSlskdAPI()
        slskd.add_transfer(
            username="peer", directory="Music\\Retry", filename=filename,
            id="same-id", state="Completed, Succeeded")

        purge_completed_transfers(_ctx(db, slskd))
        removed_before = {
            call.id for call in slskd.transfers.cancel_download_calls
            if call.remove
        }
        db.stamp_transfer_completion(
            "peer", filename, "/downloads/track.flac",
            datetime.now(timezone.utc), transfer_id="same-id")
        purge_completed_transfers(_ctx(db, slskd))
        removed_after = {
            call.id for call in slskd.transfers.cancel_download_calls
            if call.remove
        }

        assert_failure_stamp_success_gate(
            removed_before, removed_after, "same-id")

    @given(state=st.sampled_from(_TERMINAL_STATES[1:]))
    def test_missing_requested_at_never_claims_newer_retry(self, state):
        db = FakePipelineDB()
        filename = "Music\\Retry\\track.flac"
        base = datetime(2026, 7, 13, tzinfo=timezone.utc)
        db.record_transfer_enqueue([
            TransferLedgerRow(
                request_id=1, username="peer", filename=filename),
        ])
        row = next(iter(db._transfer_ledger.values()))
        row.enqueued_at = base + timedelta(minutes=3)
        slskd = FakeSlskdAPI()
        slskd.add_transfer(
            username="peer", directory="Music\\Retry", filename=filename,
            id="old-terminal", state=state, requestedAt=None,
            enqueuedAt=(base + timedelta(minutes=4)).isoformat())
        slskd.add_transfer(
            username="peer", directory="Music\\Retry", filename=filename,
            id="new-retry", state="InProgress",
            requestedAt=(base + timedelta(minutes=3)).isoformat())

        purge_completed_transfers(_ctx(db, slskd))

        self.assertIsNone(row.transfer_id)
        self.assertIsNone(row.completed_at)
        self.assertEqual(slskd.transfers.cancel_download_calls, [])

    @given(
        ledger_count=st.integers(min_value=1, max_value=6),
        repeat_count=st.integers(min_value=1, max_value=6),
    )
    def test_repeated_claim_of_same_transfer_id_mutates_one_row(
        self, ledger_count, repeat_count,
    ):
        db = FakePipelineDB()
        requested_at = datetime(2026, 7, 13, tzinfo=timezone.utc)
        for index in range(ledger_count):
            db.record_transfer_enqueue([
                TransferLedgerRow(
                    request_id=index + 1, username="peer",
                    filename="Music\\same.flac"),
            ])
            newest = max(db._transfer_ledger.values(), key=lambda row: row.id)
            newest.enqueued_at = requested_at - timedelta(minutes=1)
        claim = TerminalFailureClaim(
            transfer_id="same-id", username="peer",
            filename="Music\\same.flac", requested_at=requested_at)

        for _ in range(repeat_count):
            db.claim_terminal_failures([claim], requested_at)

        assert_transfer_id_claim_is_globally_unique(
            "same-id",
            [row.transfer_id for row in db._transfer_ledger.values()],
        )


class TestPurgeCheckersTripOnViolations(unittest.TestCase):
    """Known-bad self-tests: each checker must trip on a planted
    violating removal set — an untested checker is unfalsifiable."""

    def test_foreign_checker_trips_when_a_foreign_transfer_is_stamped(self):
        worlds = (CompletedTransferWorld(
            key=0, state="Completed, Succeeded", ownership="foreign"),)
        with self.assertRaises(AssertionError):
            assert_foreign_never_mutated(
                worlds, stamped_ids={"t-0"}, removed_ids=set())

    def test_nonterminal_checker_trips_when_live_transfer_is_removed(self):
        worlds = (CompletedTransferWorld(
            key=0, state="InProgress", ownership="unstamped"),)
        with self.assertRaises(AssertionError):
            assert_nonterminal_never_mutated(
                worlds, newly_stamped_ids=set(), removed_ids={"t-0"})

    def test_success_gate_checker_trips_when_unstamped_success_is_stamped(self):
        worlds = (CompletedTransferWorld(
            key=0, state="Completed, Succeeded", ownership="unstamped"),)
        with self.assertRaises(AssertionError):
            assert_unstamped_success_waits_for_event(
                worlds, newly_stamped_ids={"t-0"}, removed_ids=set())

    def test_failure_checker_trips_when_owned_failure_survives(self):
        worlds = (CompletedTransferWorld(
            key=0, state="Completed, Errored", ownership="unstamped"),)
        with self.assertRaises(AssertionError):
            assert_owned_terminal_failure_is_stamped_and_removed(
                worlds, newly_stamped_ids=set(), removed_ids=set())

    def test_one_to_one_claim_count_would_trip_on_expanded_ownership(self):
        with self.assertRaises(AssertionError):
            assert_one_to_one_claiming(
                ledger_count=1,
                failure_count=2,
                claimed_ids={"failure-0", "failure-1"},
                removed_ids={"failure-0", "failure-1"},
            )

    def test_failed_write_checker_trips_on_removal(self):
        with self.assertRaises(AssertionError):
            assert_failed_stamp_write_fails_closed(
                "failure", newly_stamped_ids=set(), removed_ids={"failure"})

    def test_failure_stamp_success_checker_trips_on_early_removal(self):
        with self.assertRaises(AssertionError):
            assert_failure_stamp_success_gate(
                removed_before_event={"same-id"},
                removed_after_event={"same-id"},
                transfer_id="same-id",
            )

    def test_terminal_accounting_checker_trips_on_overlap(self):
        worlds = (CompletedTransferWorld(
            key=0, state="Completed, Errored", ownership="foreign"),)
        with self.assertRaises(AssertionError):
            assert_terminal_summary_is_disjoint(
                worlds,
                removed=0,
                success_waiting=0,
                failure_unconfirmed=1,
                foreign=1,
            )

    def test_global_transfer_id_checker_trips_on_duplicate_claim(self):
        with self.assertRaises(AssertionError):
            assert_transfer_id_claim_is_globally_unique(
                "same-id", ["same-id", "same-id"])

    def test_p3_checker_trips_when_a_removable_record_survives(self):
        worlds = (CompletedTransferWorld(
            key=0, state="Completed, Succeeded", ownership="stamped"),)
        with self.assertRaises(AssertionError):
            assert_stamped_owned_completed_is_removed(worlds, removed_ids=set())

    def test_p3_checker_passes_live_stamped_transfer_left_alone(self):
        # A stamped-owned transfer that hasn't reached a terminal state
        # yet is NOT a P3 target (nothing to remove) — the checker must
        # not raise here.
        worlds = (CompletedTransferWorld(
            key=0, state="InProgress", ownership="stamped"),)
        assert_stamped_owned_completed_is_removed(worlds, removed_ids=set())


if __name__ == "__main__":
    unittest.main()
