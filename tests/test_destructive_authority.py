#!/usr/bin/env python3
"""Deterministic authority and importer-race pins for destructive actions."""

from __future__ import annotations

import unittest
import threading
import os
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

from beets import library

from lib.destructive_release_service import (
    BanSourceImporterBusy,
    BanSourceLockContended,
    BanSourceReleaseMismatch,
    BanSourceRequest,
    BanSourceSuccess,
    BanSourceTransitionConflict,
    DeleteImporterBusy,
    DeleteLockContended,
    DeleteReleaseMismatch,
    DeleteRequest,
    DeleteSuccess,
    ban_source,
    delete_release_from_library,
)
from lib.beets_db import BeetsDB
from lib.beets_delete import (
    BeetsDeleteCompleted,
    BeetsDeleteRequest,
    run_beets_delete,
)
from lib.pipeline_db import (
    ADVISORY_LOCK_NAMESPACE_IMPORT,
    ADVISORY_LOCK_NAMESPACE_RELEASE,
    PipelineDB,
    release_id_to_lock_key,
)
from lib.import_queue import IMPORT_JOB_AUTOMATION
from lib.transitions import TransitionConflict, TransitionConflictKind
from tests.fakes import FakeBeetsDB, FakePipelineDB
from tests.helpers import make_request_row
from tests.test_pipeline_db import TEST_DSN, make_db, requires_postgres


RELEASE_A = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
RELEASE_B = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
DISCOGS_A = "12856590"
MALFORMED_ID = "malformed-provider-id"


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

    def test_dual_canonical_request_identity_fails_closed(self) -> None:
        self.db.seed_request(make_request_row(
            id=41,
            status="imported",
            mb_release_id=RELEASE_A,
            discogs_release_id=DISCOGS_A,
        ))

        result = ban_source(
            pipeline_db=self.db,
            beets_db=self.beets,
            request=BanSourceRequest(request_id=41),
        )

        self.assertIsInstance(result, BanSourceReleaseMismatch)
        self._assert_no_mutation()

    def test_nonempty_malformed_request_identity_fails_before_release_lock(
        self,
    ) -> None:
        for mb_release_id, discogs_release_id in (
            (MALFORMED_ID, DISCOGS_A),
            (RELEASE_A, MALFORMED_ID),
        ):
            with self.subTest(
                mb_release_id=mb_release_id,
                discogs_release_id=discogs_release_id,
            ):
                self.db = FakePipelineDB()
                self.db.seed_request(make_request_row(
                    id=41,
                    status="imported",
                    mb_release_id=mb_release_id,
                    discogs_release_id=discogs_release_id,
                ))
                self.beets = FakeBeetsDB()

                result = ban_source(
                    pipeline_db=self.db,
                    beets_db=self.beets,
                    request=BanSourceRequest(request_id=41),
                )

                self.assertIsInstance(result, BanSourceReleaseMismatch)
                self.assertEqual(
                    self.db.advisory_lock_calls,
                    [(ADVISORY_LOCK_NAMESPACE_IMPORT, 41)],
                )
                self._assert_no_mutation()

    def test_lifecycle_cas_conflict_precedes_every_destructive_effect(self) -> None:
        conflict = TransitionConflict(
            request_id=41,
            target_status="wanted",
            kind=TransitionConflictKind.stale_source,
            expected_status="imported",
            actual_status="replaced",
        )

        result = ban_source(
            pipeline_db=self.db,
            beets_db=self.beets,
            request=BanSourceRequest(request_id=41),
            finalize_request_fn=lambda *_args, **_kwargs: conflict,
        )

        self.assertIsInstance(result, BanSourceTransitionConflict)
        self._assert_no_mutation()

    def test_unsearchable_bad_rip_preserves_search_stop(self) -> None:
        """Bad Rip bans the source and removes the copy without searching."""
        self.db.seed_request(make_request_row(
            id=41,
            status="unsearchable",
            mb_release_id=RELEASE_A,
            imported_path="/Beets/Artist/Album",
        ))
        self.db.log_download(
            request_id=41,
            soulseek_username="bad-peer",
            outcome="success",
        )

        result = ban_source(
            pipeline_db=self.db,
            beets_db=self.beets,
            request=BanSourceRequest(request_id=41),
        )

        self.assertNotIsInstance(result, BanSourceTransitionConflict)
        assert isinstance(result, BanSourceSuccess)
        self.assertEqual(result.request_status, "unsearchable")
        row = self.db.request(41)
        self.assertEqual(row["status"], "unsearchable")
        self.assertIsNone(row["imported_path"])
        self.assertEqual(
            [(entry.request_id, entry.username) for entry in self.db.denylist],
            [(41, "bad-peer")],
        )
        self.assertEqual(self.db.download_logs[-1].outcome, "curator_ban")


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

    def test_dual_canonical_album_identity_fails_closed_in_every_pipeline_world(
        self,
    ) -> None:
        """A Beets row naming two pressings can never select a delete target."""
        for pipeline_world in ("mb", "discogs", "neither", "both"):
            with self.subTest(pipeline_world=pipeline_world):
                db = FakePipelineDB()
                if pipeline_world in ("mb", "both"):
                    db.seed_request(make_request_row(
                        id=41,
                        status="imported",
                        mb_release_id=RELEASE_A,
                    ))
                if pipeline_world in ("discogs", "both"):
                    db.seed_request(make_request_row(
                        id=42,
                        status="imported",
                        mb_release_id=DISCOGS_A,
                        discogs_release_id=DISCOGS_A,
                    ))
                    db.enqueue_import_job(
                        IMPORT_JOB_AUTOMATION,
                        request_id=42,
                        dedupe_key="automation_import:request:42",
                    )
                beets = FakeBeetsDB()
                beets.set_album_detail(7, {
                    **_album(),
                    "discogs_albumid": DISCOGS_A,
                })

                result = delete_release_from_library(
                    pipeline_db=db,
                    beets_db=beets,
                    request=DeleteRequest(album_id=7, purge_pipeline=True),
                )

                self.assertIsInstance(result, DeleteReleaseMismatch)
                self.assertIsNotNone(beets.get_album_detail(7))
                if pipeline_world in ("mb", "both"):
                    self.assertIsNotNone(db.get_request(41))
                if pipeline_world in ("discogs", "both"):
                    self.assertIsNotNone(db.get_request(42))

    def test_nonempty_malformed_album_identity_fails_before_any_lock(self) -> None:
        for mb_albumid, discogs_albumid in (
            (MALFORMED_ID, DISCOGS_A),
            (RELEASE_A, MALFORMED_ID),
        ):
            with self.subTest(
                mb_albumid=mb_albumid,
                discogs_albumid=discogs_albumid,
            ):
                self.db = FakePipelineDB()
                self.db.seed_request(make_request_row(
                    id=41,
                    status="imported",
                    mb_release_id=RELEASE_A,
                ))
                self.beets = FakeBeetsDB()
                self.beets.set_album_detail(7, {
                    **_album(),
                    "mb_albumid": mb_albumid,
                    "discogs_albumid": discogs_albumid,
                })

                result = delete_release_from_library(
                    pipeline_db=self.db,
                    beets_db=self.beets,
                    request=DeleteRequest(album_id=7, purge_pipeline=True),
                )

                self.assertIsInstance(result, DeleteReleaseMismatch)
                self.assertEqual(self.db.advisory_lock_calls, [])
                self.assertIsNotNone(self.beets.get_album_detail(7))
                self.assertIsNotNone(self.db.get_request(41))


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

    def test_library_delete_no_pipeline_honors_release_lock(self) -> None:
        db1 = make_db()
        beets = FakeBeetsDB()
        beets.set_album_detail(7, _album())
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
                        return delete_release_from_library(
                            pipeline_db=db2,
                            beets_db=beets,
                            request=DeleteRequest(album_id=7),
                        )
                    finally:
                        db2.close()

                with ThreadPoolExecutor(max_workers=1) as pool:
                    future = pool.submit(contend)
                    barrier.wait(timeout=5)
                    result = future.result(timeout=5)

            self.assertIsInstance(result, DeleteLockContended)
            self.assertIsNotNone(beets.get_album_detail(7))
        finally:
            db1.close()

    def test_library_delete_pipeline_lock_then_active_job_both_fail_closed(
        self,
    ) -> None:
        db1 = make_db()
        request_id = db1.add_request(
            "Artist A",
            "Album A",
            "request",
            mb_release_id=DISCOGS_A,
            discogs_release_id=DISCOGS_A,
            status="imported",
        )
        db1.enqueue_import_job(
            IMPORT_JOB_AUTOMATION,
            request_id=request_id,
            dedupe_key=f"automation_import:request:{request_id}",
        )
        beets = FakeBeetsDB()
        beets.set_album_detail(7, {
            **_album(release_id=DISCOGS_A),
            "discogs_albumid": DISCOGS_A,
        })
        barrier = threading.Barrier(2)
        try:
            with db1.advisory_lock(
                ADVISORY_LOCK_NAMESPACE_IMPORT,
                request_id,
            ) as acquired:
                self.assertTrue(acquired)

                def contend() -> object:
                    assert TEST_DSN is not None
                    db2 = PipelineDB(TEST_DSN)
                    try:
                        barrier.wait(timeout=5)
                        return delete_release_from_library(
                            pipeline_db=db2,
                            beets_db=beets,
                            request=DeleteRequest(album_id=7),
                        )
                    finally:
                        db2.close()

                with ThreadPoolExecutor(max_workers=1) as pool:
                    future = pool.submit(contend)
                    barrier.wait(timeout=5)
                    contended = future.result(timeout=5)

            self.assertIsInstance(contended, DeleteLockContended)
            assert TEST_DSN is not None
            db2 = PipelineDB(TEST_DSN)
            try:
                busy = delete_release_from_library(
                    pipeline_db=db2,
                    beets_db=beets,
                    request=DeleteRequest(album_id=7),
                )
            finally:
                db2.close()
            self.assertIsInstance(busy, DeleteImporterBusy)
            self.assertIsNotNone(db1.get_request(request_id))
            self.assertIsNotNone(beets.get_album_detail(7))
        finally:
            db1.close()

    def test_both_importer_locks_remain_held_during_beets_mutation(
        self,
    ) -> None:
        db1 = make_db()
        request_id = db1.add_request(
            "Artist A",
            "Album A",
            "request",
            mb_release_id=RELEASE_A,
            status="imported",
        )
        entered_delete = threading.Event()
        allow_delete = threading.Event()

        beets = FakeBeetsDB()
        beets.set_album_detail(7, _album())
        notifier_saw_released_locks = False

        def blocking_delete(request: BeetsDeleteRequest) -> BeetsDeleteCompleted:
            entered_delete.set()
            if not allow_delete.wait(timeout=5):
                raise TimeoutError("test did not release Beets mutation")
            beets._album_detail.pop(request.album_id)
            return BeetsDeleteCompleted(
                album_id=request.album_id,
                album_name="Album",
                artist_name="Artist",
                former_album_path="/music/Artist/Album",
                deleted_tracks=0,
                deleted_artifacts=0,
                preserved_paths=(),
            )

        def notify_after_release(_path: str):
            nonlocal notifier_saw_released_locks
            assert TEST_DSN is not None
            observer = PipelineDB(TEST_DSN)
            try:
                with observer.advisory_lock(
                    ADVISORY_LOCK_NAMESPACE_IMPORT, request_id,
                ) as import_free:
                    with observer.advisory_lock(
                        ADVISORY_LOCK_NAMESPACE_RELEASE,
                        release_id_to_lock_key(RELEASE_A),
                    ) as release_free:
                        notifier_saw_released_locks = import_free and release_free
            finally:
                observer.close()
            return ()
        try:
            def destroy() -> object:
                assert TEST_DSN is not None
                db2 = PipelineDB(TEST_DSN)
                try:
                    return delete_release_from_library(
                        pipeline_db=db2,
                        beets_db=beets,
                        request=DeleteRequest(album_id=7),
                        beets_delete_fn=blocking_delete,
                        notify_fn=notify_after_release,
                    )
                finally:
                    db2.close()

            with ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(destroy)
                self.assertTrue(entered_delete.wait(timeout=5))
                with db1.advisory_lock(
                    ADVISORY_LOCK_NAMESPACE_IMPORT,
                    request_id,
                ) as acquired_during_delete:
                    self.assertFalse(acquired_during_delete)
                with db1.advisory_lock(
                    ADVISORY_LOCK_NAMESPACE_RELEASE,
                    release_id_to_lock_key(RELEASE_A),
                ) as release_acquired_during_delete:
                    self.assertFalse(release_acquired_during_delete)
                allow_delete.set()
                result = future.result(timeout=5)

            self.assertIsInstance(result, DeleteSuccess)
            self.assertTrue(notifier_saw_released_locks)
            self.assertIsNone(beets.get_album_detail(7))
            self.assertIsNotNone(db1.get_request(request_id))
        finally:
            allow_delete.set()
            db1.close()

    def test_real_pg_and_pinned_beets_delete_slice(self) -> None:
        db = make_db()
        try:
            request_id = db.add_request(
                "Artist A", "Album A", "request",
                mb_release_id=RELEASE_A, status="imported",
            )
            with tempfile.TemporaryDirectory() as raw:
                root = Path(raw) / "library"
                album_dir = root / "Artist A" / "Album A"
                album_dir.mkdir(parents=True)
                track = album_dir / "01 Track.flac"
                track.write_bytes(b"audio")
                sidecar = album_dir / "cratedigger.json"
                sidecar.write_bytes(b"sidecar")
                db_path = Path(raw) / "beets.db"
                config_dir = Path(raw) / "config"
                config_dir.mkdir()
                (config_dir / "config.yaml").write_text(
                    f"directory: {root}\n"
                    f"library: {db_path}\n"
                    "plugins: []\n"
                    "clutter: ['cratedigger.json']\n",
                    encoding="utf-8",
                )
                runtime_config = Path(raw) / "config.ini"
                runtime_config.write_text(
                    "[Beets]\n"
                    f"directory = {root}\n"
                    f"config_dir = {config_dir}\n"
                    f"python = {sys.executable}\n",
                    encoding="utf-8",
                )
                beets_lib = library.Library(str(db_path), str(root))
                item = library.Item(
                    path=str(track.relative_to(root)),
                    album="Album A", albumartist="Artist A",
                    artist="Artist A", title="Track", mb_albumid=RELEASE_A,
                )
                album = beets_lib.add_album([item])
                album_id = int(album.id)
                beets_lib._close()
                with (
                    patch.dict(os.environ, {
                        "CRATEDIGGER_RUNTIME_CONFIG": str(runtime_config),
                    }),
                    BeetsDB(str(db_path), library_root=str(root)) as beets,
                ):
                    result = delete_release_from_library(
                        pipeline_db=db,
                        beets_db=beets,
                        request=DeleteRequest(
                            album_id=album_id,
                            purge_pipeline=True,
                            expected_pipeline_id=request_id,
                            expected_release_id=RELEASE_A,
                        ),
                        beets_delete_fn=run_beets_delete,
                        notify_fn=lambda _path: (),
                    )

                self.assertIsInstance(result, DeleteSuccess)
                self.assertFalse(track.exists())
                self.assertFalse(sidecar.exists())
                self.assertIsNone(db.get_request(request_id))
                with BeetsDB(str(db_path), library_root=str(root)) as beets:
                    self.assertIsNone(beets.get_album_detail(album_id))
        finally:
            db.close()


if __name__ == "__main__":
    unittest.main()
