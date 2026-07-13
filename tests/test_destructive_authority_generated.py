#!/usr/bin/env python3
"""Generated no-mutation laws for destructive release authority."""

from __future__ import annotations

import copy
import unittest
import uuid
from dataclasses import dataclass

from hypothesis import given, strategies as st

import tests._hypothesis_profiles  # noqa: F401
from lib.destructive_release_service import (
    BanSourceSuccess,
    BanSourceRequest,
    DeleteRequest,
    DeleteSuccess,
    ban_source,
    delete_release_from_library,
)
from lib.import_queue import IMPORT_JOB_AUTOMATION
from lib.pipeline_db import (
    ADVISORY_LOCK_NAMESPACE_IMPORT,
    ADVISORY_LOCK_NAMESPACE_RELEASE,
)
from tests.fakes import FakeBeetsDB, FakePipelineDB
from tests.helpers import make_request_row


@dataclass(frozen=True)
class DestructiveState:
    requests: tuple[tuple[int, dict[str, object] | None], ...]
    denylist: tuple[object, ...]
    hashes: tuple[object, ...]
    logs: tuple[object, ...]
    album: dict[str, object] | None
    delete_calls: tuple[int, ...]


def snapshot_state(
    db: FakePipelineDB,
    beets: FakeBeetsDB,
    *,
    request_ids: tuple[int, ...],
    album_id: int,
) -> DestructiveState:
    return DestructiveState(
        requests=tuple(
            (request_id, copy.deepcopy(db.get_request(request_id)))
            for request_id in request_ids
        ),
        denylist=tuple(copy.deepcopy(db.denylist)),
        hashes=tuple(copy.deepcopy(db.bad_audio_hashes)),
        logs=tuple(copy.deepcopy(db.download_logs)),
        album=copy.deepcopy(beets.get_album_detail(album_id)),
        delete_calls=tuple(beets.delete_album_calls),
    )


def assert_rejection_preserved_state(
    before: DestructiveState,
    after: DestructiveState,
    *,
    rejected: bool,
) -> None:
    """Rejecting a destructive request must preserve all owned state."""
    if rejected and before != after:
        raise AssertionError("destructive rejection mutated owned state")


def _different_release(release_id: str) -> str:
    value = uuid.UUID(release_id)
    return str(uuid.UUID(int=value.int ^ 1))


def _configure_lock_world(
    db: FakePipelineDB,
    *,
    request_id: int,
    lock_failure: str,
    job_race: bool,
) -> None:
    job_inserted = False

    def acquire(namespace: int, _key: int) -> bool:
        nonlocal job_inserted
        if lock_failure == "import" and namespace == ADVISORY_LOCK_NAMESPACE_IMPORT:
            return False
        if lock_failure == "release" and namespace == ADVISORY_LOCK_NAMESPACE_RELEASE:
            return False
        if (
            job_race
            and not job_inserted
            and namespace == ADVISORY_LOCK_NAMESPACE_RELEASE
        ):
            job_inserted = True
            db.enqueue_import_job(
                IMPORT_JOB_AUTOMATION,
                request_id=request_id,
                dedupe_key=f"automation_import:request:{request_id}",
            )
        return True

    db.set_advisory_lock_result(acquire)


class TestGeneratedDestructiveAuthority(unittest.TestCase):
    @given(
        release_uuid=st.uuids().map(str),
        identity_matches=st.booleans(),
        lock_failure=st.sampled_from(("none", "import", "release")),
        job_race=st.booleans(),
    )
    def test_ban_rejections_are_zero_mutation(
        self,
        release_uuid: str,
        identity_matches: bool,
        lock_failure: str,
        job_race: bool,
    ) -> None:
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=41,
            status="imported",
            mb_release_id=release_uuid,
        ))
        beets = FakeBeetsDB()
        _configure_lock_world(
            db,
            request_id=41,
            lock_failure=lock_failure,
            job_race=job_race,
        )
        expected = release_uuid if identity_matches else _different_release(release_uuid)
        before = snapshot_state(db, beets, request_ids=(41,), album_id=7)

        result = ban_source(
            pipeline_db=db,
            beets_db=beets,
            request=BanSourceRequest(41, expected),
        )

        after = snapshot_state(db, beets, request_ids=(41,), album_id=7)
        assert_rejection_preserved_state(
            before,
            after,
            rejected=not isinstance(result, BanSourceSuccess),
        )

    @given(
        release_uuid=st.uuids().map(str),
        release_confirmation=st.sampled_from(("same", "different", "absent")),
        pipeline_confirmation=st.sampled_from(("same", "different", "absent")),
        lock_failure=st.sampled_from(("none", "import", "release")),
        job_race=st.booleans(),
    )
    def test_library_delete_rejections_are_zero_mutation(
        self,
        release_uuid: str,
        release_confirmation: str,
        pipeline_confirmation: str,
        lock_failure: str,
        job_race: bool,
    ) -> None:
        other = _different_release(release_uuid)
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=41, status="imported", mb_release_id=release_uuid,
        ))
        db.seed_request(make_request_row(
            id=42, status="imported", mb_release_id=other,
        ))
        beets = FakeBeetsDB()
        beets.set_album_detail(7, {
            "id": 7,
            "album": "A",
            "artist": "B",
            "mb_albumid": release_uuid,
            "discogs_albumid": None,
            "tracks": [],
        })
        _configure_lock_world(
            db,
            request_id=41,
            lock_failure=lock_failure,
            job_race=job_race,
        )
        expected_release = {
            "same": release_uuid,
            "different": other,
            "absent": None,
        }[release_confirmation]
        expected_pipeline = {
            "same": 41,
            "different": 42,
            "absent": None,
        }[pipeline_confirmation]
        before = snapshot_state(db, beets, request_ids=(41, 42), album_id=7)

        result = delete_release_from_library(
            pipeline_db=db,
            beets_db=beets,
            request=DeleteRequest(
                album_id=7,
                purge_pipeline=True,
                expected_pipeline_id=expected_pipeline,
                expected_release_id=expected_release,
            ),
        )

        after = snapshot_state(db, beets, request_ids=(41, 42), album_id=7)
        assert_rejection_preserved_state(
            before,
            after,
            rejected=not isinstance(result, DeleteSuccess),
        )


class TestDestructiveAuthorityCheckerKnownBad(unittest.TestCase):
    def test_checker_rejects_planted_mutation(self) -> None:
        before = DestructiveState((), (), (), (), None, ())
        after = DestructiveState((), ("planted denylist write",), (), (), None, ())
        with self.assertRaisesRegex(AssertionError, "mutated owned state"):
            assert_rejection_preserved_state(before, after, rejected=True)

    def test_checker_ignores_success_state_changes(self) -> None:
        before = DestructiveState((), (), (), (), None, ())
        after = DestructiveState((), (), (), (), None, (7,))
        assert_rejection_preserved_state(before, after, rejected=False)


if __name__ == "__main__":
    unittest.main()
