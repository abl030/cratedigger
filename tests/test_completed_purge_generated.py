#!/usr/bin/env python3
"""Generated invariants for terminal slskd transfer cleanup.

The write-ahead-ledgered ``(username, filename)`` queue key becomes destructive
authority only after slskd accepts the POST. slskd may then assign any number
of attempt-local IDs to that one queue entry; every terminal successor remains
owned. Pending, foreign, and nonterminal transfers are never mutated.

Deterministic pins live in ``tests.test_download`` and ``tests.test_repair``.
Profiles and promotion policy: tests/_hypothesis_profiles.py and
docs/generated-testing.md.
"""
from __future__ import annotations

import configparser
import os
import sys
import unittest
from collections.abc import Iterator
from dataclasses import dataclass
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import tests._hypothesis_profiles  # noqa: F401

from hypothesis import given
from hypothesis import strategies as st

from lib.config import CratediggerConfig
from lib.context import CratediggerContext
from lib.pipeline_db import TransferLedgerRow
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
_REMOVAL_OUTCOMES = ("success", "false", "exception")
_OWNERSHIP_STATES = ("foreign", "pending", "confirmed")


@dataclass(frozen=True)
class TransferAttemptWorld:
    index: int
    state: str
    removal_outcome: str


@dataclass(frozen=True)
class CompletedTransferWorld:
    key: int
    ownership: str
    attempts: tuple[TransferAttemptWorld, ...]


@st.composite
def completed_transfer_worlds(draw) -> tuple[CompletedTransferWorld, ...]:
    key_count = draw(st.integers(min_value=0, max_value=6))
    worlds: list[CompletedTransferWorld] = []
    for key in range(key_count):
        attempt_count = draw(st.integers(min_value=1, max_value=4))
        attempts = tuple(
            TransferAttemptWorld(
                index=index,
                state=draw(st.sampled_from(_LIVE_STATES + _TERMINAL_STATES)),
                removal_outcome=draw(st.sampled_from(_REMOVAL_OUTCOMES)),
            )
            for index in range(attempt_count)
        )
        worlds.append(CompletedTransferWorld(
            key=key,
            ownership=draw(st.sampled_from(_OWNERSHIP_STATES)),
            attempts=attempts,
        ))
    return tuple(worlds)


def _username(key: int) -> str:
    return f"peer{key}"


def _filename(key: int) -> str:
    return f"Music\\Album{key}\\track.flac"


def _transfer_id(key: int, attempt: int) -> str:
    return f"successor-{key}-{attempt}"


def _ctx(
    db: FakePipelineDB,
    slskd: FakeSlskdAPI,
) -> CratediggerContext:
    return CratediggerContext(
        cfg=CratediggerConfig.from_ini(configparser.ConfigParser()),
        slskd=slskd,
        pipeline_db_source=FakePipelineDBSource(db),
    )


def _build_world(
    worlds: tuple[CompletedTransferWorld, ...],
) -> tuple[FakePipelineDB, FakeSlskdAPI]:
    db = FakePipelineDB()
    slskd = FakeSlskdAPI()
    for world in worlds:
        username = _username(world.key)
        filename = _filename(world.key)
        if world.ownership != "foreign":
            db.record_transfer_enqueue([
                TransferLedgerRow(
                    request_id=world.key + 1,
                    username=username,
                    filename=filename,
                ),
            ])
        if world.ownership == "confirmed":
            db.confirm_transfer_enqueue(username, filename)
        for attempt in world.attempts:
            transfer_id = _transfer_id(world.key, attempt.index)
            slskd.add_transfer(
                username=username,
                directory=f"Music\\Album{world.key}",
                filename=filename,
                id=transfer_id,
                state=attempt.state,
            )
            if attempt.removal_outcome == "false":
                slskd.transfers.cancel_download_results_by_id[transfer_id] = False
            elif attempt.removal_outcome == "exception":
                slskd.transfers.cancel_download_errors_by_id[transfer_id] = (
                    RuntimeError("remove failed")
                )
    return db, slskd


def _attempts(
    worlds: tuple[CompletedTransferWorld, ...],
) -> Iterator[tuple[CompletedTransferWorld, TransferAttemptWorld]]:
    for world in worlds:
        for attempt in world.attempts:
            yield world, attempt


def assert_successor_attempts_removed(
    worlds: tuple[CompletedTransferWorld, ...],
    removed_ids: set[str],
) -> None:
    """Every removable terminal successor of a confirmed key disappears."""
    for world, attempt in _attempts(worlds):
        if not (
            world.ownership == "confirmed"
            and attempt.state.startswith("Completed,")
            and attempt.removal_outcome == "success"
        ):
            continue
        transfer_id = _transfer_id(world.key, attempt.index)
        if transfer_id not in removed_ids:
            raise AssertionError(
                f"owned terminal successor {transfer_id!r} was left behind"
            )


def assert_only_confirmed_terminal_attempted(
    worlds: tuple[CompletedTransferWorld, ...],
    attempted_ids: set[str],
) -> None:
    expected = {
        _transfer_id(world.key, attempt.index)
        for world, attempt in _attempts(worlds)
        if world.ownership == "confirmed"
        and attempt.state.startswith("Completed,")
    }
    if attempted_ids != expected:
        raise AssertionError(
            f"terminal cleanup attempted {attempted_ids!r}, expected {expected!r}"
        )


class TestCompletedPurgeGenerated(unittest.TestCase):
    @given(completed_transfer_worlds())
    def test_only_confirmed_terminal_transfers_are_attempted(
        self,
        worlds: tuple[CompletedTransferWorld, ...],
    ) -> None:
        db, slskd = _build_world(worlds)

        with patch("lib.slskd_transfers.logger"):
            summary = purge_completed_transfers(_ctx(db, slskd))

        attempted_ids = {
            call.id for call in slskd.transfers.cancel_download_calls
        }
        assert_only_confirmed_terminal_attempted(worlds, attempted_ids)

        attempts = list(_attempts(worlds))
        owned_terminal = [
            attempt
            for world, attempt in attempts
            if world.ownership == "confirmed"
            and attempt.state.startswith("Completed,")
        ]
        expected_removed = sum(
            attempt.removal_outcome == "success"
            for attempt in owned_terminal
        )
        expected_failed = len(owned_terminal) - expected_removed
        expected_foreign = sum(
            world.ownership != "confirmed"
            and attempt.state.startswith("Completed,")
            for world, attempt in attempts
        )
        expected_nonterminal = sum(
            not attempt.state.startswith("Completed,")
            for _, attempt in attempts
        )

        self.assertEqual(summary.removed, expected_removed)
        self.assertEqual(summary.removal_failed, expected_failed)
        self.assertEqual(summary.foreign_count, expected_foreign)
        self.assertEqual(summary.nonterminal_count, expected_nonterminal)
        self.assertEqual(
            summary.removed + summary.removal_failed + summary.foreign_count,
            sum(
                attempt.state.startswith("Completed,")
                for _, attempt in attempts
            ),
        )
        self.assertTrue(
            all(call.remove for call in slskd.transfers.cancel_download_calls)
        )

    @given(completed_transfer_worlds())
    def test_every_successor_id_for_a_confirmed_key_remains_owned(
        self,
        worlds: tuple[CompletedTransferWorld, ...],
    ) -> None:
        db, slskd = _build_world(worlds)

        with patch("lib.slskd_transfers.logger"):
            purge_completed_transfers(_ctx(db, slskd))

        remaining_ids = {
            transfer.id
            for user in slskd.transfers.get_all_downloads()
            for directory in user.directories
            for transfer in directory.files
        }
        all_ids = {
            _transfer_id(world.key, attempt.index)
            for world, attempt in _attempts(worlds)
        }
        assert_successor_attempts_removed(worlds, all_ids - remaining_ids)

    def test_successful_cleanup_is_idempotent(self) -> None:
        world = CompletedTransferWorld(
            key=0,
            ownership="confirmed",
            attempts=tuple(
                TransferAttemptWorld(
                    index=index,
                    state=state,
                    removal_outcome="success",
                )
                for index, state in enumerate(_TERMINAL_STATES)
            ),
        )
        db, slskd = _build_world((world,))
        ctx = _ctx(db, slskd)

        first = purge_completed_transfers(ctx)
        first_call_count = len(slskd.transfers.cancel_download_calls)
        second = purge_completed_transfers(ctx)

        self.assertEqual(first.removed, len(world.attempts))
        self.assertEqual(second.removed, 0)
        self.assertEqual(
            len(slskd.transfers.cancel_download_calls), first_call_count
        )


class TestCompletedPurgeGeneratedKnownBad(unittest.TestCase):
    def test_successor_checker_trips_when_retry_is_left_behind(self) -> None:
        world = CompletedTransferWorld(
            key=0,
            ownership="confirmed",
            attempts=(TransferAttemptWorld(
                index=0,
                state="Completed, Errored",
                removal_outcome="success",
            ),),
        )
        with self.assertRaisesRegex(AssertionError, "left behind"):
            assert_successor_attempts_removed((world,), set())

    def test_scope_checker_trips_on_pending_intent_removal(self) -> None:
        world = CompletedTransferWorld(
            key=0,
            ownership="pending",
            attempts=(TransferAttemptWorld(
                index=0,
                state="Completed, Succeeded",
                removal_outcome="success",
            ),),
        )
        with self.assertRaisesRegex(AssertionError, "expected"):
            assert_only_confirmed_terminal_attempted(
                (world,), {_transfer_id(world.key, 0)}
            )


if __name__ == "__main__":
    unittest.main()
