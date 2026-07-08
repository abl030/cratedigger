#!/usr/bin/env python3
"""Generated request-lifecycle tests — issue #548 follow-up.

A Hypothesis ``RuleBasedStateMachine`` drives random sequences of REAL
production transitions — ``lib/transitions.py::finalize_request`` (the
single status-mutation funnel) and ``PipelineDB.supersede_request_mbid``
(Replace) — against a ``FakePipelineDB``, then asserts the archivist
lifecycle invariants after every step:

* every row's status stays in the legal set (the CHECK constraint's
  vocabulary — the fake doesn't enforce it, so the machine must);
* ``replaced`` rows are terminal and FROZEN — nothing mutates them after
  the supersede, ever (the request is the source of truth; operator
  actions supersede rows, they don't rewrite them);
* request identity (mb_release_id / source / created_at) is immutable
  for every row from creation onward;
* every replaced row has a linked descendant pointing back via
  ``replaces_request_id`` (unique structurally — a replaced row cannot be
  superseded again) — Replace creates a new row, not a mutation;
* ``active_download_state`` exists only on ``downloading`` rows;
* the guarded transitions really guard: a download claim on a non-wanted
  row and a downloading→wanted requeue on a non-downloading row are
  no-ops that leave the row untouched.

Rule eligibility mirrors production caller guards: the DB-guarded
operations (claim, download-fail requeue) deliberately target ANY row —
the guard is the contract under test — while unguarded transitions
(→imported/→manual/→wanted-reset, supersede) select rows by the statuses
their production callers act on, exactly as the live system does (the
DAG in ``VALID_TRANSITIONS`` warns but does not block; enforcement lives
in caller row-selection).

Profiles, promotion policy, fault-injection qualification:
docs/generated-testing.md.
"""

import copy
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)

from hypothesis import strategies as st
from hypothesis.stateful import (
    RuleBasedStateMachine,
    invariant,
    precondition,
    rule,
)

from lib.transitions import RequestTransition, finalize_request
from tests.fakes import FakePipelineDB
from tests.helpers import make_active_download_state_json

LEGAL_STATUSES = frozenset(
    {"wanted", "downloading", "imported", "manual", "replaced"})

_IDENTITY_FIELDS = ("mb_release_id", "source", "created_at")

_RETRY_COUNTERS = ("search_attempts", "download_attempts", "validation_attempts")


# ===========================================================================
# Invariant checkers — module functions so the known-bad self-tests below
# can prove each one trips (harness RED/GREEN).
# ===========================================================================

def assert_statuses_legal(rows: list[dict]) -> None:
    for row in rows:
        if row["status"] not in LEGAL_STATUSES:
            raise AssertionError(
                f"request {row['id']} has illegal status {row['status']!r}")


def assert_replaced_row_frozen(snapshot: dict, row: dict) -> None:
    """A replaced row is a frozen audit record — byte-identical forever."""
    if row != snapshot:
        diffs = {
            key: (snapshot.get(key), row.get(key))
            for key in set(snapshot) | set(row)
            if snapshot.get(key) != row.get(key)
        }
        raise AssertionError(
            f"replaced request {snapshot['id']} mutated after supersede: {diffs}")


def assert_identity_immutable(identity: tuple, row: dict) -> None:
    current = tuple(row[field] for field in _IDENTITY_FIELDS)
    if current != identity:
        raise AssertionError(
            f"request {row['id']} identity drifted: {identity} -> {current}")


def assert_download_state_coherent(row: dict) -> None:
    if row["status"] != "downloading" and row.get("active_download_state"):
        raise AssertionError(
            f"request {row['id']} carries active_download_state while "
            f"{row['status']!r}")


def assert_replacement_linked(replaced_id: int, descendant: dict | None) -> None:
    if descendant is None:
        raise AssertionError(
            f"replaced request {replaced_id} has no linked descendant row")
    if descendant.get("replaces_request_id") != replaced_id:
        raise AssertionError(
            f"descendant of {replaced_id} does not point back "
            f"(replaces_request_id={descendant.get('replaces_request_id')!r})")


class RequestLifecycleMachine(RuleBasedStateMachine):
    def __init__(self) -> None:
        super().__init__()
        self.db = FakePipelineDB()
        self.ids: list[int] = []
        self.identity: dict[int, tuple] = {}
        self.frozen: dict[int, dict] = {}
        self._mbid_counter = 0

    # -- helpers ------------------------------------------------------

    def _unique_mbid(self) -> str:
        self._mbid_counter += 1
        return f"lifecycle-mbid-{self._mbid_counter:04d}"

    def _row(self, request_id: int) -> dict:
        row = self.db.request(request_id)
        assert row is not None
        return row

    def _ids_with_status(self, *statuses: str) -> list[int]:
        return [
            rid for rid in self.ids
            if self._row(rid)["status"] in statuses
        ]

    def _track(self, request_id: int) -> None:
        self.ids.append(request_id)
        row = self._row(request_id)
        self.identity[request_id] = tuple(
            row[field] for field in _IDENTITY_FIELDS)

    # -- rules (production entry points only) --------------------------

    @rule()
    def create_request(self) -> None:
        rid = self.db.add_request(
            "Lifecycle Artist", f"Album {self._mbid_counter}", "request",
            mb_release_id=self._unique_mbid())
        self._track(rid)

    @precondition(lambda self: self.ids)
    @rule(data=st.data())
    def claim_download(self, data) -> None:
        """DB-guarded: succeeds only from 'wanted'; targets ANY row."""
        rid = data.draw(st.sampled_from(self.ids), label="claim target")
        before = copy.deepcopy(self._row(rid))
        ok = finalize_request(self.db, rid, RequestTransition.to_downloading(
            state_json=make_active_download_state_json([])))
        row = self._row(rid)
        if before["status"] == "wanted":
            if not ok or row["status"] != "downloading":
                raise AssertionError(
                    f"wanted->downloading claim failed: ok={ok}, "
                    f"status={row['status']!r}")
        else:
            if ok or row != before:
                raise AssertionError(
                    f"claim on {before['status']!r} row must be a no-op "
                    f"(ok={ok})")

    @precondition(lambda self: self._ids_with_status("downloading"))
    @rule(data=st.data())
    def import_success(self, data) -> None:
        rid = data.draw(
            st.sampled_from(self._ids_with_status("downloading")),
            label="import target")
        ok = finalize_request(self.db, rid, RequestTransition.to_imported(
            from_status="downloading",
            imported_path=f"/Beets/lifecycle/{rid}",
            min_bitrate=245,
        ))
        row = self._row(rid)
        if not ok or row["status"] != "imported":
            raise AssertionError(
                f"downloading->imported failed: ok={ok}, "
                f"status={row['status']!r}")

    @precondition(lambda self: self.ids)
    @rule(data=st.data())
    def download_fail_requeue(self, data) -> None:
        """DB-guarded: succeeds only from 'downloading'; targets ANY row.
        Preserves retry counters and records the failed attempt."""
        rid = data.draw(st.sampled_from(self.ids), label="requeue target")
        before = copy.deepcopy(self._row(rid))
        ok = finalize_request(self.db, rid, RequestTransition.to_wanted(
            from_status="downloading", attempt_type="download"))
        row = self._row(rid)
        if before["status"] == "downloading":
            if not ok or row["status"] != "wanted":
                raise AssertionError(
                    f"downloading->wanted requeue failed: ok={ok}, "
                    f"status={row['status']!r}")
            if row["download_attempts"] != (before["download_attempts"] or 0) + 1:
                raise AssertionError(
                    "failed download attempt was not recorded "
                    f"({before['download_attempts']} -> "
                    f"{row['download_attempts']})")
        else:
            if ok or row != before:
                raise AssertionError(
                    f"downloading->wanted on {before['status']!r} row must "
                    f"be a no-op (ok={ok})")

    @precondition(lambda self: self._ids_with_status("wanted", "downloading"))
    @rule(data=st.data())
    def flag_manual(self, data) -> None:
        rid = data.draw(
            st.sampled_from(self._ids_with_status("wanted", "downloading")),
            label="manual target")
        from_status = self._row(rid)["status"]
        ok = finalize_request(
            self.db, rid, RequestTransition.to_manual(from_status=from_status))
        row = self._row(rid)
        if not ok or row["status"] != "manual":
            raise AssertionError(
                f"{from_status}->manual failed: ok={ok}, "
                f"status={row['status']!r}")

    @precondition(lambda self: self._ids_with_status("imported", "manual"))
    @rule(data=st.data())
    def requeue_from_terminal(self, data) -> None:
        """Operator requeue (upgrade / retry-from-manual): clears counters."""
        rid = data.draw(
            st.sampled_from(self._ids_with_status("imported", "manual")),
            label="requeue-from-terminal target")
        from_status = self._row(rid)["status"]
        ok = finalize_request(
            self.db, rid, RequestTransition.to_wanted(from_status=from_status))
        row = self._row(rid)
        if not ok or row["status"] != "wanted":
            raise AssertionError(
                f"{from_status}->wanted requeue failed: ok={ok}, "
                f"status={row['status']!r}")
        for counter in _RETRY_COUNTERS:
            if row[counter] != 0:
                raise AssertionError(
                    f"requeue did not clear {counter} (={row[counter]})")

    @precondition(
        lambda self: self._ids_with_status("wanted", "imported", "manual"))
    @rule(data=st.data())
    def replace(self, data) -> None:
        """Replace (issue-#282 shape): the old row flips to 'replaced' and
        freezes; a NEW linked row is created. Downloading rows are excluded
        because the replace service converges active downloads before the
        supersede (Phase 0) — the machine mirrors that caller guard."""
        rid = data.draw(
            st.sampled_from(
                self._ids_with_status("wanted", "imported", "manual")),
            label="replace target")
        new_id = self.db.supersede_request_mbid(
            rid,
            new_mb_release_id=self._unique_mbid(),
            new_mb_release_group_id=None,
            new_mb_artist_id=None,
            new_artist_name="Lifecycle Artist",
            new_album_title=f"Album {rid} (correct pressing)",
            new_year=None,
            new_country=None,
            new_tracks=[],
        )
        self._track(new_id)
        old_row = self._row(rid)
        if old_row["status"] != "replaced":
            raise AssertionError(
                f"supersede left old row status={old_row['status']!r}")
        # The frozen audit snapshot: byte-identical from here on.
        self.frozen[rid] = copy.deepcopy(old_row)
        assert_replacement_linked(
            rid, self.db.get_request_by_replaces_request_id(rid))

    # -- invariants (checked after every rule) --------------------------

    @invariant()
    def statuses_stay_legal(self) -> None:
        assert_statuses_legal([self._row(rid) for rid in self.ids])

    @invariant()
    def replaced_rows_stay_frozen(self) -> None:
        for rid, snapshot in self.frozen.items():
            assert_replaced_row_frozen(snapshot, self._row(rid))

    @invariant()
    def identity_never_drifts(self) -> None:
        for rid, identity in self.identity.items():
            assert_identity_immutable(identity, self._row(rid))

    @invariant()
    def download_state_only_while_downloading(self) -> None:
        for rid in self.ids:
            assert_download_state_coherent(self._row(rid))

    @invariant()
    def every_replaced_row_has_a_descendant(self) -> None:
        for rid in self.frozen:
            assert_replacement_linked(
                rid, self.db.get_request_by_replaces_request_id(rid))


TestRequestLifecycleMachine = RequestLifecycleMachine.TestCase


class TestLifecycleCheckersTripOnViolations(unittest.TestCase):
    """Known-bad self-tests for every lifecycle invariant checker."""

    def test_trips_on_illegal_status(self):
        with self.assertRaises(AssertionError):
            assert_statuses_legal([{"id": 1, "status": "zombie"}])

    def test_trips_on_thawed_replaced_row(self):
        snapshot = {"id": 1, "status": "replaced", "min_bitrate": 245}
        thawed = {"id": 1, "status": "replaced", "min_bitrate": 320}
        with self.assertRaises(AssertionError):
            assert_replaced_row_frozen(snapshot, thawed)

    def test_trips_on_resurrected_replaced_row(self):
        snapshot = {"id": 1, "status": "replaced"}
        resurrected = {"id": 1, "status": "wanted"}
        with self.assertRaises(AssertionError):
            assert_replaced_row_frozen(snapshot, resurrected)

    def test_trips_on_identity_drift(self):
        row = {"id": 1, "mb_release_id": "mbid-B", "source": "request",
               "created_at": "t0"}
        with self.assertRaises(AssertionError):
            assert_identity_immutable(("mbid-A", "request", "t0"), row)

    def test_trips_on_stray_download_state(self):
        with self.assertRaises(AssertionError):
            assert_download_state_coherent(
                {"id": 1, "status": "imported",
                 "active_download_state": "{}"})

    def test_trips_on_missing_descendant(self):
        with self.assertRaises(AssertionError):
            assert_replacement_linked(1, None)
        with self.assertRaises(AssertionError):
            assert_replacement_linked(1, {"replaces_request_id": 99})


if __name__ == "__main__":
    unittest.main()
