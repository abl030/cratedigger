"""Release-association writer locking contract (#1070)."""

from __future__ import annotations

import unittest

from lib.release_identity import ReleaseIdentity
from lib.request_creation_service import (
    RequestCreationInput,
    RequestCreationService,
)
from tests.fakes import FakePipelineDB
from tests.test_pipeline_db import TEST_DSN, make_db, requires_postgres


def _identity(value: str) -> ReleaseIdentity:
    identity = ReleaseIdentity.from_id(value)
    assert identity is not None
    return identity


class TestReleaseAssociationLocks(unittest.TestCase):
    def test_deduplicates_colliding_keys_and_unwinds_on_later_contention(self) -> None:
        from lib.release_association_locks import release_identity_locks

        db = FakePipelineDB()
        db.set_advisory_lock_result(lambda _namespace, key: key != 9)

        with release_identity_locks(
            db,
            (_identity("12856590"), _identity("12856591"), _identity("12856592")),
            lock_key_fn=lambda identity: {"12856590": 3, "12856591": 9, "12856592": 3}[identity.release_id],
        ) as result:
            self.assertFalse(result.acquired)
            self.assertEqual(result.keys, (3, 9))

        self.assertEqual(db.advisory_lock_calls[-2:], [(0x52454C45, 3), (0x52454C45, 9)])
        self.assertEqual(
            db.advisory_lock_events[-4:],
            [("enter", 0x52454C45, 3), ("enter", 0x52454C45, 9),
             ("exit", 0x52454C45, 9), ("exit", 0x52454C45, 3)],
        )

    def test_locks_are_sorted_by_key_not_identity_order(self) -> None:
        from lib.release_association_locks import release_identity_locks

        db = FakePipelineDB()
        with release_identity_locks(
            db,
            (_identity("12856590"), _identity("12856591")),
            lock_key_fn=lambda identity: {"12856590": 9, "12856591": 3}[identity.release_id],
        ) as result:
            self.assertTrue(result.acquired)
            self.assertEqual(result.keys, (3, 9))

        self.assertEqual(db.advisory_lock_calls[-2:], [(0x52454C45, 3), (0x52454C45, 9)])


@requires_postgres
class TestReleaseAssociationLocksRealPostgres(unittest.TestCase):
    def test_multi_key_scope_is_session_scoped_and_releases_all_keys(self) -> None:
        from lib.pipeline_db import ADVISORY_LOCK_NAMESPACE_RELEASE, PipelineDB
        from lib.release_association_locks import release_identity_locks

        db1 = make_db()
        assert TEST_DSN is not None
        db2 = PipelineDB(TEST_DSN)
        identities = (_identity("12856590"), _identity("12856591"))
        try:
            with release_identity_locks(db1, identities) as held:
                self.assertTrue(held.acquired)
                for key in held.keys:
                    with db2.advisory_lock(
                        ADVISORY_LOCK_NAMESPACE_RELEASE, key,
                    ) as contended:
                        self.assertFalse(contended)
            for key in held.keys:
                with db2.advisory_lock(
                    ADVISORY_LOCK_NAMESPACE_RELEASE, key,
                ) as acquired:
                    self.assertTrue(acquired)
        finally:
            db1.close()
            db2.close()

    def test_zero_snapshot_blocks_direct_creation_until_release(self) -> None:
        from lib.config import CratediggerConfig
        from lib.pipeline_db import (
            ADVISORY_LOCK_NAMESPACE_RELEASE,
            PipelineDB,
            release_id_to_lock_key,
        )

        identity = "bce7d8c3-815b-449c-8e18-df806398986c"
        db1 = make_db()
        assert TEST_DSN is not None
        db2 = PipelineDB(TEST_DSN)
        try:
            with db1.advisory_lock(
                ADVISORY_LOCK_NAMESPACE_RELEASE,
                release_id_to_lock_key(identity),
            ) as held:
                self.assertTrue(held)
                result = RequestCreationService(
                    db2, CratediggerConfig(),
                ).create_or_resume(RequestCreationInput(
                    release_id=identity, artist_name="Artist", album_title="Album",
                    source="request", tracks=[], mb_release_id=identity,
                ))
                self.assertEqual(result.outcome, "busy")
                self.assertIsNone(db1.get_request_by_release_id(identity))
        finally:
            db1.close()
            db2.close()

    def test_unique_snapshot_blocks_canonical_add_until_release(self) -> None:
        from lib.canonical_release_service import OUTCOME_BUSY, CanonicalReleaseService
        from lib.pipeline_db import (
            ADVISORY_LOCK_NAMESPACE_RELEASE,
            PipelineDB,
            release_id_to_lock_key,
        )

        filed = "7aabf975-9a06-4b2e-854c-2c700380ebd5"
        loser = "4878ee47-f8b8-45c8-832c-62de3bccfa6e"
        db1 = make_db()
        db1.add_request("Filed", "Unique", "request", mb_release_id=filed)
        loser_id = db1.add_request("Merged", "Candidate", "request", mb_release_id=loser)
        assert TEST_DSN is not None
        db2 = PipelineDB(TEST_DSN)
        try:
            with db1.advisory_lock(
                ADVISORY_LOCK_NAMESPACE_RELEASE,
                release_id_to_lock_key(filed),
            ) as held:
                self.assertTrue(held)
                result = CanonicalReleaseService(
                    db2, canonical_fn=lambda _id: filed,
                ).reconcile_request(loser_id)
                self.assertEqual(result.outcome, OUTCOME_BUSY)
                current = db1.get_request(loser_id)
                assert current is not None
                self.assertIsNone(current["canonical_release_id"])
        finally:
            db1.close()
            db2.close()
