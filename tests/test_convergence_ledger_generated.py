"""Generated tests for the ledger-positive ownership flip of
``lib.slskd_transfers.converge_slskd_orphans`` (issue #571 PR 3).

Two properties over generated worlds of live slskd transfers, each
independently {foreign/pending/confirmed} x {backed/unbacked by a
``downloading`` row} x transfer state:

1. **C1 (good-citizen)** — a live transfer without a confirmed accepted
   enqueue in ``slskd_transfer_ledger`` is NEVER cancelled by convergence,
   whatever its state or backed status. This includes both foreign keys and
   pending write-ahead intents whose POST failed or had an unknown outcome.
   This is the flip:
   the OLD doctrine cancelled any transfer no ``downloading`` row backed,
   which risked cancelling a human's transfer on a shared slskd instance.
2. **C2 (housekeeping still works)** — a live (non-terminal), CONFIRMED
   transfer that is NOT backed by a currently-``downloading`` row IS
   cancelled — cratedigger's own stray (the classic Replace-abandons-
   transfer case, and a confirmed transfer whose row already self-healed
   back to ``wanted``).

Both properties drive the REAL ``converge_slskd_orphans`` entry point
over ``FakeSlskdAPI`` + ``FakePipelineDB`` — not the pure
``lib.repair.find_slskd_orphans`` helper directly — so the generated
harness also exercises the ledger-set/backed-set assembly convergence
itself owns.

The deterministic pins for these same invariants live in
``tests/test_download.py::TestConvergeSlskdOrphans`` (orchestration) and
``tests/test_repair.py::TestFindSlskdOrphans`` (pure classification).

Profiles and promotion policy: tests/_hypothesis_profiles.py and
docs/generated-testing.md.
"""
from __future__ import annotations

import configparser
import json
import os
import shutil
import sys
import tempfile
import unittest
from dataclasses import dataclass, replace
from typing import Literal

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from hypothesis import given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)
from lib.config import CratediggerConfig
from lib.context import CratediggerContext
from lib.download_ownership import DownloadOwnershipWriter
from lib.grab_list import DownloadFile
from lib.pipeline_db import TransferLedgerRow
from lib.slskd_transfers import cancel_and_delete, converge_slskd_orphans
from tests.fakes import FakePipelineDB, FakePipelineDBSource, FakeSlskdAPI
from tests.helpers import (
    make_cycle_collaborators,
    make_request_row,
    rebind_collaborators,
)

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


@dataclass(frozen=True)
class TransferWorld:
    key: int
    state: str
    ownership: Literal["foreign", "pending", "confirmed"]
    backed: bool


@st.composite
def transfer_worlds(draw) -> tuple[TransferWorld, ...]:
    count = draw(st.integers(min_value=0, max_value=6))
    worlds = []
    for i in range(count):
        worlds.append(TransferWorld(
            key=i,
            state=draw(st.sampled_from(_ALL_STATES)),
            ownership=draw(st.sampled_from(
                ("foreign", "pending", "confirmed"))),
            backed=draw(st.booleans()),
        ))
    return tuple(worlds)


def _username(key: int) -> str:
    return f"peer{key}"


def _filename(key: int) -> str:
    return f"Music\\Album{key}\\track.flac"


def _cfg() -> CratediggerConfig:
    return CratediggerConfig.from_ini(configparser.ConfigParser())


def _build_world_fakes(
    worlds: tuple[TransferWorld, ...],
) -> tuple[FakePipelineDB, FakeSlskdAPI]:
    db = FakePipelineDB()
    slskd = FakeSlskdAPI()
    ledger_rows = []
    for w in worlds:
        username, filename = _username(w.key), _filename(w.key)
        slskd.add_transfer(
            username=username, directory=f"Music\\Album{w.key}",
            filename=filename, id=f"t-{w.key}", state=w.state)
        if w.ownership != "foreign":
            ledger_rows.append(TransferLedgerRow(
                request_id=w.key + 1, username=username, filename=filename))
        if w.backed:
            db.seed_request(make_request_row(
                id=w.key + 1, status="downloading",
                active_download_state={
                    "filetype": "flac",
                    "files": [{"username": username, "filename": filename}],
                }))
    if ledger_rows:
        db.record_transfer_enqueue(ledger_rows)
    for w in worlds:
        if w.ownership == "confirmed":
            db.confirm_transfer_enqueue(
                _username(w.key), _filename(w.key),
                request_id=w.key + 1)
    return db, slskd


def _ctx(db: FakePipelineDB, slskd: FakeSlskdAPI) -> CratediggerContext:
    return CratediggerContext(
        collaborators=make_cycle_collaborators(
            cfg=_cfg(),
            slskd=slskd,
            pipeline_db_source=FakePipelineDBSource(db),
        ),
    )


# --- Invariant checkers (module-level so the known-bad self-tests can
# call them directly) --------------------------------------------------


def assert_unconfirmed_never_cancelled(
    worlds: tuple[TransferWorld, ...], cancelled_ids: set[str],
) -> None:
    """C1: a foreign or merely pending transfer is never cancelled."""
    for w in worlds:
        if w.ownership == "confirmed":
            continue
        transfer_id = f"t-{w.key}"
        if transfer_id in cancelled_ids:
            raise AssertionError(
                f"unconfirmed transfer {transfer_id!r} (world={w!r}) "
                "was cancelled by convergence")


def assert_confirmed_unbacked_live_is_cancelled(
    worlds: tuple[TransferWorld, ...], cancelled_ids: set[str],
) -> None:
    """C2: a confirmed, unbacked, LIVE (non-terminal) transfer is always
    cancelled — cratedigger's own stray."""
    for w in worlds:
        if w.ownership != "confirmed" or w.backed:
            continue
        if w.state.startswith("Completed"):
            continue
        transfer_id = f"t-{w.key}"
        if transfer_id not in cancelled_ids:
            raise AssertionError(
                f"confirmed, unbacked, live transfer {transfer_id!r} "
                f"(world={w!r}) was NOT cancelled by convergence")


# --- C3/C4: the cleanup path answers to the same ownership authority
# (issue #1278) ---------------------------------------------------------


@dataclass(frozen=True)
class CleanupWorld:
    key: int
    ownership: Literal["foreign", "pending", "confirmed"]
    stamped: bool
    has_transfer_id: bool


@st.composite
def cleanup_worlds(draw) -> tuple[CleanupWorld, ...]:
    count = draw(st.integers(min_value=0, max_value=5))
    return tuple(
        CleanupWorld(
            key=i,
            ownership=draw(st.sampled_from(
                ("foreign", "pending", "confirmed"))),
            stamped=draw(st.booleans()),
            has_transfer_id=draw(st.booleans()),
        )
        for i in range(count)
    )


def _build_cleanup_fakes(
    worlds: tuple[CleanupWorld, ...], root: str,
) -> tuple[FakePipelineDB, FakeSlskdAPI, list[DownloadFile], dict[int, str]]:
    """One payload on disk per world, reachable by BOTH path routes.

    ``stamped`` worlds carry an ingested ``local_path``; the rest are
    resolved through the fresh events page — the instance-wide feed that
    hands back whichever client last completed that key, which is why an
    unowned world here is a stranger's album rather than a hypothetical.
    """
    db = FakePipelineDB()
    slskd = FakeSlskdAPI()
    files: list[DownloadFile] = []
    paths: dict[int, str] = {}
    ledger_rows = []
    events = []
    for w in worlds:
        username, filename = _username(w.key), _filename(w.key)
        album_dir = os.path.join(root, f"Album{w.key}")
        os.makedirs(album_dir, exist_ok=True)
        path = os.path.join(album_dir, "track.flac")
        with open(path, "w") as handle:
            handle.write("audio")
        paths[w.key] = path
        files.append(DownloadFile(
            filename=filename,
            id=f"t-{w.key}" if w.has_transfer_id else "",
            file_dir=f"Music\\Album{w.key}",
            username=username,
            size=5,
            local_path=path if w.stamped else None,
        ))
        events.append(slskd.events.make_event(
            id=f"ev-{w.key}", timestamp="2026-08-26T10:00:00.0000000Z",
            type="DownloadFileComplete",
            data=json.dumps({
                "version": 0,
                "localFilename": path,
                "remoteFilename": filename,
                "transfer": {
                    "id": f"t-{w.key}", "username": username,
                    "filename": filename, "size": 5,
                },
            })))
        if w.ownership != "foreign":
            ledger_rows.append(TransferLedgerRow(
                request_id=w.key + 1, username=username, filename=filename))
    slskd.events.set_events(events)
    if ledger_rows:
        db.record_transfer_enqueue(ledger_rows)
    for w in worlds:
        if w.ownership == "confirmed":
            db.confirm_transfer_enqueue(
                _username(w.key), _filename(w.key),
                request_id=w.key + 1)
    return db, slskd, files, paths


def _cleanup_ctx(
    db: FakePipelineDB, slskd: FakeSlskdAPI, root: str,
) -> CratediggerContext:
    ctx = CratediggerContext(
        collaborators=make_cycle_collaborators(
            cfg=replace(_cfg(), slskd_download_dir=root),
            slskd=slskd,
            pipeline_db_source=FakePipelineDBSource(db),
        ),
    )
    rebind_collaborators(
        ctx,
        download_ownership=DownloadOwnershipWriter(
        db_factory=lambda: db, close_after_use=False),
    )
    return ctx


def assert_unowned_payload_survives(
    worlds: tuple[CleanupWorld, ...],
    paths: dict[int, str],
    cancelled_ids: set[str],
) -> None:
    """C3: cleanup neither cancels nor unlinks a foreign or merely
    pending key -- the same good-citizen rule C1 holds convergence to."""
    for w in worlds:
        if w.ownership == "confirmed":
            continue
        if f"t-{w.key}" in cancelled_ids:
            raise AssertionError(
                f"unconfirmed transfer 't-{w.key}' (world={w!r}) was "
                "cancelled by cleanup")
        if not os.path.exists(paths[w.key]):
            raise AssertionError(
                f"unconfirmed payload {paths[w.key]!r} (world={w!r}) was "
                "deleted by cleanup")


def assert_owned_payload_destroyed(
    worlds: tuple[CleanupWorld, ...],
    paths: dict[int, str],
    cancelled_ids: set[str],
) -> None:
    """C4: cleanup still does its job -- a confirmed key's payload is
    unlinked, and its transfer cancelled whenever it still has an ID."""
    for w in worlds:
        if w.ownership != "confirmed":
            continue
        if os.path.exists(paths[w.key]):
            raise AssertionError(
                f"confirmed payload {paths[w.key]!r} (world={w!r}) "
                "survived cleanup")
        if w.has_transfer_id and f"t-{w.key}" not in cancelled_ids:
            raise AssertionError(
                f"confirmed transfer 't-{w.key}' (world={w!r}) was NOT "
                "cancelled by cleanup")


class TestGeneratedCancelAndDeleteOwnership(unittest.TestCase):
    """C3 + C4 over generated worlds, through the REAL
    ``cancel_and_delete`` entry point and a real on-disk payload."""

    @given(worlds=cleanup_worlds())
    def test_c3_and_c4_hold_across_worlds(self, worlds):
        # The scratch root is created and removed inside the example, not
        # via addCleanup: a cleanup registered inside @given accumulates
        # one live tmpfs world per example (issue #1214).
        root = tempfile.mkdtemp(prefix="cratedigger-cleanup-world-")
        try:
            db, slskd, files, paths = _build_cleanup_fakes(worlds, root)

            cancel_and_delete(files, _cleanup_ctx(db, slskd, root))

            cancelled = {c.id for c in slskd.transfers.cancel_download_calls}
            assert_unowned_payload_survives(worlds, paths, cancelled)
            assert_owned_payload_destroyed(worlds, paths, cancelled)
        finally:
            shutil.rmtree(root, ignore_errors=True)

    @given(worlds=cleanup_worlds())
    def test_result_is_true_only_when_every_file_was_ours_and_cancelled(
        self, worlds,
    ):
        """The boolean callers read to conclude "verified no acceptance"
        is never True while a key was skipped as unowned."""
        root = tempfile.mkdtemp(prefix="cratedigger-cleanup-result-")
        try:
            db, slskd, files, _paths = _build_cleanup_fakes(worlds, root)

            ok = cancel_and_delete(files, _cleanup_ctx(db, slskd, root))

            every_file_owned_and_cancellable = all(
                w.ownership == "confirmed" and w.has_transfer_id
                for w in worlds
            )
            if ok and not every_file_owned_and_cancellable:
                raise AssertionError(
                    f"cancel_and_delete reported success for worlds={worlds!r}"
                )
        finally:
            shutil.rmtree(root, ignore_errors=True)


class TestCleanupCheckersTripOnViolations(unittest.TestCase):
    """Known-bad self-tests for C3 and C4, one per raise site."""

    def _world(self, ownership, *, has_transfer_id=True):
        return (CleanupWorld(
            key=0, ownership=ownership, stamped=True,
            has_transfer_id=has_transfer_id),)

    def test_c3_trips_when_a_foreign_transfer_is_cancelled(self):
        with self.assertRaisesRegex(AssertionError, "was cancelled by cleanup"):
            assert_unowned_payload_survives(
                self._world("foreign"), {0: "/nonexistent/kept.flac"},
                cancelled_ids={"t-0"})

    def test_c3_trips_when_a_pending_payload_is_deleted(self):
        root = tempfile.mkdtemp(prefix="cratedigger-c3-selftest-")
        self.addCleanup(shutil.rmtree, root, ignore_errors=True)
        missing = os.path.join(root, "already-deleted.flac")

        with self.assertRaisesRegex(AssertionError, "was deleted by cleanup"):
            assert_unowned_payload_survives(
                self._world("pending"), {0: missing}, cancelled_ids=set())

    def test_c4_trips_when_an_owned_payload_survives(self):
        root = tempfile.mkdtemp(prefix="cratedigger-c4-selftest-")
        self.addCleanup(shutil.rmtree, root, ignore_errors=True)
        survivor = os.path.join(root, "survivor.flac")
        with open(survivor, "w") as handle:
            handle.write("audio")

        with self.assertRaisesRegex(AssertionError, "survived cleanup"):
            assert_owned_payload_destroyed(
                self._world("confirmed"), {0: survivor},
                cancelled_ids={"t-0"})

    def test_c4_trips_when_an_owned_transfer_is_not_cancelled(self):
        with self.assertRaisesRegex(AssertionError, "was NOT cancelled"):
            assert_owned_payload_destroyed(
                self._world("confirmed"), {0: "/nonexistent/gone.flac"},
                cancelled_ids=set())

    def test_c4_ignores_an_owned_file_that_never_had_a_transfer_id(self):
        # Nothing to cancel: the checker must not raise on this world.
        assert_owned_payload_destroyed(
            self._world("confirmed", has_transfer_id=False),
            {0: "/nonexistent/gone.flac"}, cancelled_ids=set())


class TestGeneratedConvergeSlskdOrphans(unittest.TestCase):
    """C1 + C2 properties over generated worlds, through the REAL
    ``converge_slskd_orphans`` entry point."""

    @given(worlds=transfer_worlds())
    def test_c1_and_c2_hold_across_worlds(self, worlds):
        db, slskd = _build_world_fakes(worlds)

        converge_slskd_orphans(_ctx(db, slskd))

        cancelled_ids = {c.id for c in slskd.transfers.cancel_download_calls}
        assert_unconfirmed_never_cancelled(worlds, cancelled_ids)
        assert_confirmed_unbacked_live_is_cancelled(worlds, cancelled_ids)

    @given(worlds=transfer_worlds())
    def test_convergence_is_idempotent_second_pass_cancels_nothing_new(
        self, worlds,
    ):
        """A second pass over the same (now-converged) slskd state finds
        no NEW strays — the first pass's cancels already removed them
        from the live snapshot (FakeSlskdAPI mirrors slskd's own
        remove-on-cancel behavior)."""
        db, slskd = _build_world_fakes(worlds)
        converge_slskd_orphans(_ctx(db, slskd))
        first_pass_calls = list(slskd.transfers.cancel_download_calls)

        converge_slskd_orphans(_ctx(db, slskd))

        self.assertEqual(slskd.transfers.cancel_download_calls, first_pass_calls)


class TestConvergeCheckersTripOnViolations(unittest.TestCase):
    """Known-bad self-tests: each checker must trip on a planted
    violating cancellation set — an untested checker is unfalsifiable."""

    def test_c1_checker_trips_when_a_foreign_transfer_is_cancelled(self):
        worlds = (TransferWorld(
            key=0, state="InProgress", ownership="foreign", backed=False),)
        with self.assertRaises(AssertionError):
            assert_unconfirmed_never_cancelled(worlds, cancelled_ids={"t-0"})

    def test_c1_checker_trips_when_a_pending_transfer_is_cancelled(self):
        worlds = (TransferWorld(
            key=0, state="InProgress", ownership="pending", backed=False),)
        with self.assertRaises(AssertionError):
            assert_unconfirmed_never_cancelled(worlds, cancelled_ids={"t-0"})

    def test_c2_checker_trips_when_a_stray_survives_uncancelled(self):
        worlds = (TransferWorld(
            key=0, state="InProgress", ownership="confirmed", backed=False),)
        with self.assertRaises(AssertionError):
            assert_confirmed_unbacked_live_is_cancelled(
                worlds, cancelled_ids=set())

    def test_c2_checker_passes_terminal_stray_left_alone(self):
        # A terminal-state confirmed/unbacked transfer is NOT a C2 target
        # (nothing to cancel) — the checker must not raise here.
        worlds = (TransferWorld(
            key=0, state="Completed, Succeeded",
            ownership="confirmed", backed=False),)
        assert_confirmed_unbacked_live_is_cancelled(
            worlds, cancelled_ids=set())


if __name__ == "__main__":
    unittest.main()
