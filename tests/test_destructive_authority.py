#!/usr/bin/env python3
"""Deterministic authority and importer-race pins for destructive actions."""

from __future__ import annotations

import unittest
import threading
from concurrent.futures import ThreadPoolExecutor

from lib.destructive_release_service import (
    BanSourceImporterBusy,
    BanSourceLockContended,
    BanSourceReleaseMismatch,
    BanSourceRequest,
    DeleteImporterBusy,
    DeleteLockContended,
    DeleteReleaseMismatch,
    DeleteRequest,
    ban_source,
    delete_release_from_library,
)
from lib.pipeline_db import (
    ADVISORY_LOCK_NAMESPACE_IMPORT,
    ADVISORY_LOCK_NAMESPACE_RELEASE,
    PipelineDB,
    release_id_to_lock_key,
)
from lib.import_queue import IMPORT_JOB_AUTOMATION
from tests.fakes import FakeBeetsDB, FakePipelineDB
from tests.helpers import make_request_row
from tests.test_pipeline_db import TEST_DSN, make_db, requires_postgres


RELEASE_A = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
RELEASE_B = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"


def _album(album_id: int = 7, release_id: str = RELEASE_A) -> dict[str, object]:
    return {
        "id": album_id,
        "album": "Album A",
        "artist": "Artist A",
        "mb_albumid": release_id,
        "discogs_albumid": None,
        "tracks": [],
    }


class TestBanSourceAuthority(unittest.TestCase):
    def setUp(self) -> None:
        self.db = FakePipelineDB()
        self.db.seed_request(make_request_row(
            id=41,
            status="imported",
            mb_release_id=RELEASE_A,
        ))
        self.beets = FakeBeetsDB()

    def _assert_no_mutation(self) -> None:
        self.assertEqual(self.db.denylist, [])
        self.assertEqual(self.db.bad_audio_hashes, [])
        self.assertEqual(self.db.download_logs, [])
        self.assertEqual(self.beets.get_item_paths_calls, [])
        self.assertEqual(self.beets.locate_calls, [])
        row = self.db.get_request(41)
        self.assertIsNotNone(row)
        assert row is not None
        self.assertEqual(row["status"], "imported")

    def test_ab_identifier_mismatch_is_zero_mutation(self) -> None:
        result = ban_source(
            pipeline_db=self.db,
            beets_db=self.beets,
            request=BanSourceRequest(
                request_id=41,
                expected_release_id=RELEASE_B,
            ),
        )

        self.assertIsInstance(result, BanSourceReleaseMismatch)
        self._assert_no_mutation()

    def test_release_lock_contention_is_zero_mutation(self) -> None:
        self.db.set_advisory_lock_result(
            lambda namespace, _key: namespace != ADVISORY_LOCK_NAMESPACE_RELEASE,
        )

        result = ban_source(
            pipeline_db=self.db,
            beets_db=self.beets,
            request=BanSourceRequest(request_id=41),
        )

        self.assertIsInstance(result, BanSourceLockContended)
        self.assertEqual(
            [namespace for namespace, _key in self.db.advisory_lock_calls],
            [ADVISORY_LOCK_NAMESPACE_IMPORT, ADVISORY_LOCK_NAMESPACE_RELEASE],
        )
        self._assert_no_mutation()

    def test_job_claimed_after_release_lock_is_rechecked_under_lock(self) -> None:
        def acquire(namespace: int, _key: int) -> bool:
            if namespace == ADVISORY_LOCK_NAMESPACE_RELEASE:
                self.db.enqueue_import_job(
                    IMPORT_JOB_AUTOMATION,
                    request_id=41,
                    dedupe_key="automation_import:request:41",
                )
            return True

        self.db.set_advisory_lock_result(acquire)

        result = ban_source(
            pipeline_db=self.db,
            beets_db=self.beets,
            request=BanSourceRequest(request_id=41),
        )

        self.assertIsInstance(result, BanSourceImporterBusy)
        self._assert_no_mutation()


class TestLibraryDeleteAuthority(unittest.TestCase):
    def setUp(self) -> None:
        self.db = FakePipelineDB()
        self.db.seed_request(make_request_row(
            id=41,
            status="imported",
            mb_release_id=RELEASE_A,
        ))
        self.db.seed_request(make_request_row(
            id=42,
            status="imported",
            mb_release_id=RELEASE_B,
        ))
        self.beets = FakeBeetsDB()
        self.beets.set_album_detail(7, _album())

    def _assert_no_mutation(self) -> None:
        self.assertIsNotNone(self.beets.get_album_detail(7))
        self.assertIsNotNone(self.db.get_request(41))
        self.assertIsNotNone(self.db.get_request(42))
        self.assertEqual(self.beets.delete_album_calls, [])

    def test_ab_pipeline_identifier_mismatch_is_zero_mutation(self) -> None:
        result = delete_release_from_library(
            pipeline_db=self.db,
            beets_db=self.beets,
            request=DeleteRequest(
                album_id=7,
                purge_pipeline=True,
                expected_pipeline_id=42,
            ),
        )

        self.assertIsInstance(result, DeleteReleaseMismatch)
        self._assert_no_mutation()

    def test_release_identifier_mismatch_is_zero_mutation(self) -> None:
        result = delete_release_from_library(
            pipeline_db=self.db,
            beets_db=self.beets,
            request=DeleteRequest(
                album_id=7,
                expected_release_id=RELEASE_B,
            ),
        )

        self.assertIsInstance(result, DeleteReleaseMismatch)
        self._assert_no_mutation()

    def test_import_lock_contention_is_zero_mutation(self) -> None:
        self.db.set_advisory_lock_result(
            lambda namespace, _key: namespace != ADVISORY_LOCK_NAMESPACE_IMPORT,
        )

        result = delete_release_from_library(
            pipeline_db=self.db,
            beets_db=self.beets,
            request=DeleteRequest(album_id=7),
        )

        self.assertIsInstance(result, DeleteLockContended)
        self._assert_no_mutation()

    def test_job_claimed_after_release_lock_is_rechecked_under_lock(self) -> None:
        def acquire(namespace: int, _key: int) -> bool:
            if namespace == ADVISORY_LOCK_NAMESPACE_RELEASE:
                self.db.enqueue_import_job(
                    IMPORT_JOB_AUTOMATION,
                    request_id=41,
                    dedupe_key="automation_import:request:41",
                )
            return True

        self.db.set_advisory_lock_result(acquire)

        result = delete_release_from_library(
            pipeline_db=self.db,
            beets_db=self.beets,
            request=DeleteRequest(album_id=7),
        )

        self.assertIsInstance(result, DeleteImporterBusy)
        self._assert_no_mutation()


@requires_postgres
class TestDestructiveAuthorityRealPostgres(unittest.TestCase):
    """Two real sessions prove service contention before beets mutation."""

    def test_barrier_controlled_release_lock_blocks_destructive_service(self) -> None:
        db1 = make_db()
        request_id = db1.add_request(
            "Artist A",
            "Album A",
            "request",
            mb_release_id=RELEASE_A,
            status="imported",
        )
        beets = FakeBeetsDB()
        barrier = threading.Barrier(2)
        try:
            with db1.advisory_lock(
                ADVISORY_LOCK_NAMESPACE_RELEASE,
                release_id_to_lock_key(RELEASE_A),
            ) as acquired:
                self.assertTrue(acquired)

                def contend() -> object:
                    assert TEST_DSN is not None
                    db2 = PipelineDB(TEST_DSN)
                    try:
                        barrier.wait(timeout=5)
                        return ban_source(
                            pipeline_db=db2,
                            beets_db=beets,
                            request=BanSourceRequest(request_id),
                        )
                    finally:
                        db2.close()

                with ThreadPoolExecutor(max_workers=1) as pool:
                    future = pool.submit(contend)
                    barrier.wait(timeout=5)
                    result = future.result(timeout=5)

            self.assertIsInstance(result, BanSourceLockContended)
            row = db1.get_request(request_id)
            assert row is not None
            self.assertEqual(row["status"], "imported")
            self.assertEqual(beets.get_item_paths_calls, [])
            self.assertEqual(beets.locate_calls, [])
        finally:
            db1.close()


if __name__ == "__main__":
    unittest.main()
