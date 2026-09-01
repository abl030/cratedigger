"""Self-tests for the FakePipelineDB slskd transfer-ledger cluster.

Split out of ``tests/test_fakes.py`` (#1313) so each cluster of the
fake has its sibling tests beside it.
"""
import unittest
from datetime import UTC, datetime, timedelta

from lib.pipeline_db import (
    TransferLedgerRow,
)
from tests.fakes import (
    FakePipelineDB,
)
from tests.helpers import (
    make_request_row,
)


class TestFakePipelineDBTransferLedger(unittest.TestCase):
    """Self-tests for the slskd transfer write-ahead ownership ledger
    stubs (migration 045, issue #571)."""

    def test_record_transfer_enqueue_preserves_state(self):
        db = FakePipelineDB()
        db.record_transfer_enqueue([
            TransferLedgerRow(
                request_id=42, username="peer0", filename="Music\\a.flac",
                attempt_fingerprint="abcd1234"),
        ])
        rows = list(db._transfer_ledger.values())
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].request_id, 42)
        self.assertEqual(rows[0].username, "peer0")
        self.assertEqual(rows[0].filename, "Music\\a.flac")
        self.assertEqual(rows[0].attempt_fingerprint, "abcd1234")
        self.assertIsNone(rows[0].accepted_at)
        self.assertIsNone(rows[0].local_path)
        self.assertEqual(len(db.record_transfer_enqueue_calls), 1)

    def test_record_transfer_enqueue_empty_list_is_a_noop(self):
        db = FakePipelineDB()
        db.record_transfer_enqueue([])  # must not raise
        self.assertEqual(db._transfer_ledger, {})

    def test_record_transfer_enqueue_writes_one_row_per_file(self):
        db = FakePipelineDB()
        db.record_transfer_enqueue([
            TransferLedgerRow(request_id=1, username="p0", filename="a.flac"),
            TransferLedgerRow(request_id=1, username="p0", filename="b.flac"),
        ])
        self.assertEqual(len(db._transfer_ledger), 2)

    def test_stamp_transfer_completion_stamps_matching_row(self):
        db = FakePipelineDB()
        db.record_transfer_enqueue([
            TransferLedgerRow(request_id=1, username="p0", filename="a.flac"),
        ])
        db.confirm_transfer_enqueue("p0", "a.flac", request_id=1)
        stamped = db.stamp_transfer_completion(
            "p0", "a.flac", "/downloads/complete/a.flac")
        self.assertEqual(stamped, 1)
        row = next(iter(db._transfer_ledger.values()))
        self.assertEqual(row.local_path, "/downloads/complete/a.flac")
        self.assertIsNotNone(row.accepted_at)

    def test_confirm_transfer_enqueue_owns_newest_pending_row(self):
        db = FakePipelineDB()
        row = TransferLedgerRow(
            request_id=1, username="p0", filename="a.flac")
        db.record_transfer_enqueue([row, row])
        old_id = min(db._transfer_ledger)

        self.assertEqual(
            db.confirm_transfer_enqueue("p0", "a.flac", request_id=1), 1)

        accepted = [
            item for item in db._transfer_ledger.values()
            if item.accepted_at is not None
        ]
        self.assertEqual(len(accepted), 1)
        self.assertNotEqual(accepted[0].id, old_id)
        self.assertEqual(db.get_owned_transfer_keys(), {("p0", "a.flac")})

    def test_confirm_transfer_enqueue_never_promotes_a_sibling_request(self):
        """#1278 item 2: the fake mirrors PG's request scoping.

        Without it the fake would answer more generously than production
        -- promoting whichever request's pending row happened to be
        newest -- and every ownership-gated test built on it would be a
        fiction. Real-PG twin:
        ``tests.test_pipeline_db.TestTransferLedgerRoundTrip::
        test_confirm_transfer_enqueue_never_promotes_a_sibling_request``.
        """
        db = FakePipelineDB()
        db.record_transfer_enqueue([
            TransferLedgerRow(request_id=1, username="p0", filename="a.flac"),
        ])
        # The sibling's intent is NEWER, so an unscoped confirm lands on it.
        db.record_transfer_enqueue([
            TransferLedgerRow(request_id=2, username="p0", filename="a.flac"),
        ])

        self.assertEqual(
            db.confirm_transfer_enqueue("p0", "a.flac", request_id=1), 1)

        by_request = {
            row.request_id: row.accepted_at
            for row in db._transfer_ledger.values()
        }
        self.assertIsNotNone(by_request[1])
        self.assertIsNone(by_request[2])

    def test_confirm_transfer_enqueue_is_zero_for_a_request_with_no_intent(self):
        db = FakePipelineDB()
        db.record_transfer_enqueue([
            TransferLedgerRow(request_id=2, username="p0", filename="a.flac"),
        ])

        self.assertEqual(
            db.confirm_transfer_enqueue("p0", "a.flac", request_id=1), 0)
        self.assertEqual(db.get_owned_transfer_keys(), set())

    def test_confirm_transfer_enqueue_records_every_call_it_is_asked(self):
        """The recorder covers the promote-nothing case too -- which is
        the whole reason it exists (#1278 item 2): a caller that must not
        even ASK leaves no other trace."""
        db = FakePipelineDB()
        db.record_transfer_enqueue([
            TransferLedgerRow(request_id=1, username="p0", filename="a.flac"),
        ])

        db.confirm_transfer_enqueue("p0", "a.flac", request_id=1)
        db.confirm_transfer_enqueue("p9", "nope.flac", request_id=7)

        self.assertEqual(
            db.confirm_transfer_enqueue_calls,
            [("p0", "a.flac", 1), ("p9", "nope.flac", 7)])

    def test_confirm_transfer_enqueue_calls_start_empty(self):
        self.assertEqual(FakePipelineDB().confirm_transfer_enqueue_calls, [])

    def test_stamp_transfer_completion_unledgered_pair_is_a_noop(self):
        db = FakePipelineDB()
        stamped = db.stamp_transfer_completion(
            "foreign-peer", "foreign.flac", "/downloads/x")
        self.assertEqual(stamped, 0)
        self.assertEqual(db.get_owned_local_paths(), set())

    def test_stamp_transfer_completion_prefers_newest_open_row(self):
        # Two retries for the same (username, filename): only the newest
        # not-yet-stamped row gets the completion stamp.
        db = FakePipelineDB()
        db.record_transfer_enqueue([
            TransferLedgerRow(request_id=1, username="p0", filename="a.flac"),
        ])
        old_id = next(iter(db._transfer_ledger))
        db._transfer_ledger[old_id].enqueued_at = (
            datetime.now(UTC) - timedelta(minutes=10))
        db.record_transfer_enqueue([
            TransferLedgerRow(request_id=1, username="p0", filename="a.flac"),
        ])
        db.confirm_transfer_enqueue("p0", "a.flac", request_id=1)
        db.stamp_transfer_completion(
            "p0", "a.flac", "/downloads/complete/a.flac")
        rows = db._transfer_ledger.values()
        stamped_rows = [r for r in rows if r.local_path is not None]
        self.assertEqual(len(stamped_rows), 1)
        self.assertNotEqual(stamped_rows[0].id, old_id)

    def test_get_owned_local_paths_only_returns_stamped_rows(self):
        db = FakePipelineDB()
        db.record_transfer_enqueue([
            TransferLedgerRow(request_id=1, username="p0", filename="a.flac"),
            TransferLedgerRow(request_id=1, username="p0", filename="b.flac"),
        ])
        db.confirm_transfer_enqueue("p0", "a.flac", request_id=1)
        db.stamp_transfer_completion(
            "p0", "a.flac", "/downloads/a.flac")
        self.assertEqual(db.get_owned_local_paths(), {"/downloads/a.flac"})

    def test_get_abandoned_owned_local_paths_selects_only_wanted_without_state(self):
        from tests.helpers import make_request_row

        cases = [
            ("wanted, no state -> abandoned", "wanted", None, True),
            ("wanted, holding state", "wanted", {"files": []}, False),
            ("imported", "imported", None, False),
            ("processing", "processing", None, False),
            ("downloading", "downloading", None, False),
        ]
        for desc, status, state, expected in cases:
            with self.subTest(desc=desc):
                db = FakePipelineDB()
                db.seed_request(make_request_row(
                    id=1, status=status, active_download_state=state))
                db.record_transfer_enqueue([
                    TransferLedgerRow(
                        request_id=1, username="p0", filename="a.flac"),
                ])
                db.confirm_transfer_enqueue("p0", "a.flac", request_id=1)
                db.stamp_transfer_completion(
                    "p0", "a.flac", "/downloads/a.flac")

                paths = db.get_abandoned_owned_local_paths()

                self.assertEqual(
                    paths, {"/downloads/a.flac"} if expected else set())

    def test_get_abandoned_owned_local_paths_ignores_unstamped_rows(self):
        from tests.helpers import make_request_row

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=1, status="wanted", active_download_state=None))
        db.record_transfer_enqueue([
            TransferLedgerRow(request_id=1, username="p0", filename="a.flac"),
        ])
        db.confirm_transfer_enqueue("p0", "a.flac", request_id=1)

        self.assertEqual(db.get_abandoned_owned_local_paths(), set())

    def test_get_owned_transfer_keys_for_intersects_with_the_asked_keys(self):
        db = FakePipelineDB()
        db.record_transfer_enqueue([
            TransferLedgerRow(request_id=1, username="p0", filename="a.flac"),
            TransferLedgerRow(request_id=1, username="p1", filename="b.flac"),
        ])
        db.confirm_transfer_enqueue("p0", "a.flac", request_id=1)
        db.confirm_transfer_enqueue("p1", "b.flac", request_id=1)

        self.assertEqual(
            db.get_owned_transfer_keys_for([("p0", "a.flac")]),
            {("p0", "a.flac")},
        )
        self.assertEqual(
            db.get_owned_transfer_keys_for([("stranger", "a.flac")]), set())
        self.assertEqual(db.get_owned_transfer_keys_for([]), set())

    def test_get_owned_transfer_keys_for_excludes_pending_intent(self):
        db = FakePipelineDB()
        db.record_transfer_enqueue([
            TransferLedgerRow(request_id=1, username="p0", filename="a.flac"),
        ])

        self.assertEqual(
            db.get_owned_transfer_keys_for([("p0", "a.flac")]), set())

        db.confirm_transfer_enqueue("p0", "a.flac", request_id=1)

        self.assertEqual(
            db.get_owned_transfer_keys_for([("p0", "a.flac")]),
            {("p0", "a.flac")},
        )

    def test_get_owned_transfer_keys_empty_before_any_record(self):
        self.assertEqual(FakePipelineDB().get_owned_transfer_keys(), set())

    def test_get_owned_transfer_keys_excludes_pending_intent(self):
        db = FakePipelineDB()
        db.record_transfer_enqueue([
            TransferLedgerRow(request_id=1, username="p0", filename="a.flac"),
            TransferLedgerRow(request_id=1, username="p0", filename="a.flac"),
            TransferLedgerRow(request_id=2, username="p1", filename="b.flac"),
        ])
        db.confirm_transfer_enqueue("p0", "a.flac", request_id=1)
        db.stamp_transfer_completion(
            "p0", "a.flac", "/downloads/a.flac")
        self.assertEqual(
            db.get_owned_transfer_keys(),
            {("p0", "a.flac")})

    def test_prune_transfer_ledger_keeps_accepted_active_request_rows(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="downloading"))
        db.record_transfer_enqueue([
            TransferLedgerRow(request_id=1, username="p0", filename="a.flac"),
        ])
        db.confirm_transfer_enqueue("p0", "a.flac", request_id=1)
        old_id = next(iter(db._transfer_ledger))
        db._transfer_ledger[old_id].enqueued_at = (
            datetime.now(UTC) - timedelta(days=200))

        removed = db.prune_transfer_ledger(
            older_than=datetime.now(UTC) - timedelta(days=90))

        self.assertEqual(removed, 0)
        self.assertIn(old_id, db._transfer_ledger)

    def test_prune_transfer_ledger_removes_pending_active_request_rows(self):
        db = FakePipelineDB()
        for request_id, status in ((1, "wanted"), (2, "downloading")):
            db.seed_request(make_request_row(id=request_id, status=status))
            db.record_transfer_enqueue([
                TransferLedgerRow(
                    request_id=request_id,
                    username=f"p{request_id}",
                    filename=f"{request_id}.flac",
                ),
            ])
            ledger_id = next(
                fake_id for fake_id, row in db._transfer_ledger.items()
                if row.request_id == request_id
            )
            db._transfer_ledger[ledger_id].enqueued_at = (
                datetime.now(UTC) - timedelta(days=200))

        removed = db.prune_transfer_ledger(
            older_than=datetime.now(UTC) - timedelta(days=90))

        self.assertEqual(removed, 2)
        self.assertEqual(db._transfer_ledger, {})

    def test_prune_transfer_ledger_removes_old_terminal_rows(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="imported"))
        db.record_transfer_enqueue([
            TransferLedgerRow(request_id=1, username="p0", filename="a.flac"),
        ])
        old_id = next(iter(db._transfer_ledger))
        db._transfer_ledger[old_id].enqueued_at = (
            datetime.now(UTC) - timedelta(days=200))

        removed = db.prune_transfer_ledger(
            older_than=datetime.now(UTC) - timedelta(days=90))

        self.assertEqual(removed, 1)
        self.assertNotIn(old_id, db._transfer_ledger)

    def test_prune_transfer_ledger_keeps_rows_inside_retention(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="imported"))
        db.record_transfer_enqueue([
            TransferLedgerRow(request_id=1, username="p0", filename="a.flac"),
        ])

        removed = db.prune_transfer_ledger(
            older_than=datetime.now(UTC) - timedelta(days=90))

        self.assertEqual(removed, 0)

    def test_prune_transfer_ledger_treats_missing_request_as_inactive(self):
        # A request_id whose row no longer exists (hard-deleted elsewhere)
        # can never come back to wanted/downloading -- prunable.
        db = FakePipelineDB()
        db.record_transfer_enqueue([
            TransferLedgerRow(request_id=999, username="p0", filename="a.flac"),
        ])
        old_id = next(iter(db._transfer_ledger))
        db._transfer_ledger[old_id].enqueued_at = (
            datetime.now(UTC) - timedelta(days=200))

        removed = db.prune_transfer_ledger(
            older_than=datetime.now(UTC) - timedelta(days=90))

        self.assertEqual(removed, 1)

    def _seed_accepted_row(
        self, db: FakePipelineDB, *, request_id: int, status: str,
        username: str, filename: str,
    ) -> None:
        from tests.helpers import make_request_row

        db.seed_request(make_request_row(id=request_id, status=status))
        db.record_transfer_enqueue([
            TransferLedgerRow(
                request_id=request_id, username=username, filename=filename),
        ])
        db.confirm_transfer_enqueue(
            username, filename, request_id=request_id)

    def test_get_conflicting_transfer_request_ids_empty_keys_is_a_noop(self):
        db = FakePipelineDB()
        self.assertEqual(
            db.get_conflicting_transfer_request_ids([], exclude_request_id=1),
            set(),
        )

    def test_get_conflicting_transfer_request_ids_downloading_owner_conflicts(self):
        db = FakePipelineDB()
        self._seed_accepted_row(
            db, request_id=99, status="downloading",
            username="p0", filename="a.flac")

        conflicting = db.get_conflicting_transfer_request_ids(
            [("p0", "a.flac")], exclude_request_id=1)

        self.assertEqual(conflicting, {99})

    def test_get_conflicting_transfer_request_ids_missing_fingerprint_key_blocks(
        self,
    ):
        """#1199 item 2 fake twin: an active_download_state that EXISTS
        but lacks "attempt_fingerprint" fails CLOSED unconditionally --
        both an old (30-day) and a current accepted row block, with no
        attempt-boundary rescue by age. Equivalence note: this replaces
        test_get_conflicting_transfer_request_ids_scopes_to_current_
        attempt, which asserted the OLD row did NOT block under the
        now-deleted deploy-window time-predicate fallback; that
        differentiation no longer exists in production."""
        from tests.helpers import make_request_row

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=99, status="downloading"))
        db.record_transfer_enqueue([
            TransferLedgerRow(request_id=99, username="OLD", filename="old.flac"),
        ])
        db.confirm_transfer_enqueue("OLD", "old.flac", request_id=99)
        old_id = next(
            fid for fid, r in db._transfer_ledger.items()
            if r.username == "OLD")
        db._transfer_ledger[old_id].enqueued_at = (
            datetime.now(UTC) - timedelta(days=30))

        db.request(99)["active_download_state"] = {
            "filetype": "flac", "enqueued_at": datetime.now(UTC).isoformat(),
            "files": [],
        }
        db.record_transfer_enqueue([
            TransferLedgerRow(request_id=99, username="NEW", filename="new.flac"),
        ])
        db.confirm_transfer_enqueue("NEW", "new.flac", request_id=99)

        self.assertEqual(
            db.get_conflicting_transfer_request_ids(
                [("OLD", "old.flac")], exclude_request_id=1),
            {99},
            "missing fingerprint key fails closed regardless of age",
        )
        self.assertEqual(
            db.get_conflicting_transfer_request_ids(
                [("NEW", "new.flac")], exclude_request_id=1),
            {99},
            "missing fingerprint key still blocks the current key too",
        )

    def test_get_conflicting_transfer_request_ids_null_state_fails_closed(self):
        """No active_download_state at all (never seeded) -- every
        accepted row for the 'downloading' owner still blocks."""
        db = FakePipelineDB()
        self._seed_accepted_row(
            db, request_id=99, status="downloading",
            username="p0", filename="a.flac")

        conflicting = db.get_conflicting_transfer_request_ids(
            [("p0", "a.flac")], exclude_request_id=1)

        self.assertEqual(conflicting, {99})

    def test_get_conflicting_transfer_request_ids_explicit_json_null_fingerprint_blocks(
        self,
    ):
        """Fake parity twin for the real-PG hostile-shape pin (issue #1199
        review F8): an explicit ``"attempt_fingerprint": None`` value
        (mirroring an explicit JSON ``null``, distinct from the key being
        absent) fails closed exactly like a missing key. ``_attempt_
        fingerprint_from_state`` already handles this correctly --
        ``dict.get`` returns ``None`` for an explicit ``None`` value the
        same as for a missing key, and the ``isinstance(value, str)``
        check treats both as "no fingerprint" -- so this test proves that
        existing behaviour rather than changing it."""
        from tests.helpers import make_request_row

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=99, status="downloading"))
        db.record_transfer_enqueue([
            TransferLedgerRow(
                request_id=99, username="p0", filename="a.flac",
                attempt_fingerprint="deadbeef"),
        ])
        db.confirm_transfer_enqueue("p0", "a.flac", request_id=99)
        db.request(99)["active_download_state"] = {
            "filetype": "flac", "enqueued_at": datetime.now(UTC).isoformat(),
            "files": [], "attempt_fingerprint": None,
        }

        conflicting = db.get_conflicting_transfer_request_ids(
            [("p0", "a.flac")], exclude_request_id=1)

        self.assertEqual(conflicting, {99})

    def test_get_conflicting_transfer_request_ids_status_filter(self):
        # 'processing' included specifically to kill the
        # status-filter-widened mutant (#1178 PR2 review F1); every other
        # status here already happened to leave it unreachable.
        for status in ("wanted", "imported", "replaced", "processing"):
            with self.subTest(status=status):
                db = FakePipelineDB()
                self._seed_accepted_row(
                    db, request_id=99, status=status,
                    username="p0", filename="a.flac")

                conflicting = db.get_conflicting_transfer_request_ids(
                    [("p0", "a.flac")], exclude_request_id=1)

                self.assertEqual(conflicting, set())

    def test_get_conflicting_transfer_request_ids_excludes_own_rows(self):
        db = FakePipelineDB()
        self._seed_accepted_row(
            db, request_id=1, status="downloading",
            username="p0", filename="a.flac")

        conflicting = db.get_conflicting_transfer_request_ids(
            [("p0", "a.flac")], exclude_request_id=1)

        self.assertEqual(conflicting, set())

    def test_get_conflicting_transfer_request_ids_ignores_pending_intent(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=99, status="downloading"))
        db.record_transfer_enqueue([
            TransferLedgerRow(request_id=99, username="p0", filename="a.flac"),
        ])  # never confirmed -- accepted_at stays NULL

        conflicting = db.get_conflicting_transfer_request_ids(
            [("p0", "a.flac")], exclude_request_id=1)

        self.assertEqual(conflicting, set())

    def test_get_conflicting_transfer_request_ids_ignores_unrelated_keys(self):
        db = FakePipelineDB()
        self._seed_accepted_row(
            db, request_id=99, status="downloading",
            username="p0", filename="a.flac")

        conflicting = db.get_conflicting_transfer_request_ids(
            [("p0", "b.flac")], exclude_request_id=1)

        self.assertEqual(conflicting, set())

    def test_get_conflicting_transfer_request_ids_missing_request_row(self):
        db = FakePipelineDB()
        db.record_transfer_enqueue([
            TransferLedgerRow(request_id=99, username="p0", filename="a.flac"),
        ])
        db.confirm_transfer_enqueue("p0", "a.flac", request_id=99)

        conflicting = db.get_conflicting_transfer_request_ids(
            [("p0", "a.flac")], exclude_request_id=1)

        self.assertEqual(conflicting, set())


if __name__ == "__main__":
    unittest.main()
