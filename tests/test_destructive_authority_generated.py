#!/usr/bin/env python3
"""Generated no-mutation laws for destructive release authority."""

from __future__ import annotations

import copy
import tempfile
import unittest
import uuid
from dataclasses import dataclass
from pathlib import Path

from hypothesis import example, given, strategies as st

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
from lib.release_identity import ReleaseIdentity
from tests.fakes import DenylistEntry, FakeBeetsDB, FakePipelineDB
from tests.helpers import make_request_row


@dataclass(frozen=True)
class DestructiveState:
    requests: tuple[tuple[int, dict[str, object] | None], ...]
    denylist: tuple[object, ...]
    hashes: tuple[object, ...]
    logs: tuple[object, ...]
    album: dict[str, object] | None
    delete_calls: tuple[int, ...]
    files: tuple[tuple[str, bytes], ...]
    directories: tuple[str, ...]


def snapshot_state(
    db: FakePipelineDB,
    beets: FakeBeetsDB,
    *,
    request_ids: tuple[int, ...],
    album_id: int,
    filesystem_root: Path | None = None,
) -> DestructiveState:
    files: tuple[tuple[str, bytes], ...] = ()
    directories: tuple[str, ...] = ()
    if filesystem_root is not None and filesystem_root.exists():
        files = tuple(sorted(
            (str(path.relative_to(filesystem_root)), path.read_bytes())
            for path in filesystem_root.rglob("*")
            if path.is_file()
        ))
        directories = tuple(sorted(
            str(path.relative_to(filesystem_root))
            for path in filesystem_root.rglob("*")
            if path.is_dir()
        ))
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
        files=files,
        directories=directories,
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
    identity = ReleaseIdentity.from_id(release_id)
    assert identity is not None
    if identity.source == "musicbrainz":
        value = uuid.UUID(release_id)
        return str(uuid.UUID(int=value.int ^ 1))
    return str(int(release_id) + 1)


MB_RELEASE_IDS = st.uuids().map(str)
DISCOGS_RELEASE_IDS = st.integers(min_value=1, max_value=2_000_000_000).map(str)
INVALID_SERVER_IDS = st.text(max_size=24).map(lambda suffix: f"invalid:{suffix}")
ABSENT_SENTINELS = st.sampled_from((None, "", " ", 0, "0", " 0 "))
IDENTITY_SHAPES = (
    "mb",
    "discogs",
    "discogs_dual_layout",
    "dual",
    "invalid_primary_valid_discogs",
    "valid_mb_invalid_secondary",
    "invalid_only",
    "sentinel_primary_valid_discogs",
    "valid_mb_sentinel_secondary",
    "sentinel_only",
)
VALID_AUTHORITY_SHAPES = {
    "mb",
    "discogs",
    "discogs_dual_layout",
    "sentinel_primary_valid_discogs",
    "valid_mb_sentinel_secondary",
}


def _identity_fields(
    shape: str,
    *,
    mb_id: str,
    discogs_id: str,
    invalid_id: str,
    sentinel: object | None,
) -> tuple[object | None, object | None]:
    return {
        "mb": (mb_id, None),
        "discogs": (None, discogs_id),
        "discogs_dual_layout": (discogs_id, discogs_id),
        "dual": (mb_id, discogs_id),
        "invalid_primary_valid_discogs": (invalid_id, discogs_id),
        "valid_mb_invalid_secondary": (mb_id, invalid_id),
        "invalid_only": (invalid_id, None),
        "sentinel_primary_valid_discogs": (sentinel, discogs_id),
        "valid_mb_sentinel_secondary": (mb_id, sentinel),
        "sentinel_only": (sentinel, None),
    }[shape]


def _authoritative_release(
    shape: str,
    *,
    mb_id: str,
    discogs_id: str,
) -> str | None:
    if shape not in VALID_AUTHORITY_SHAPES:
        return None
    if shape in ("mb", "valid_mb_sentinel_secondary"):
        return mb_id
    return discogs_id


def _configure_lock_world(
    db: FakePipelineDB,
    *,
    request_id: int | None,
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
            request_id is not None
            and job_race
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
    @example(
        mb_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        discogs_id="12856590",
        invalid_id="invalid:provider",
        sentinel="0",
        identity_shape="invalid_primary_valid_discogs",
        identity_matches=True,
        lock_failure="none",
        job_race=False,
    )
    @example(
        mb_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        discogs_id="12856590",
        invalid_id="invalid:provider",
        sentinel="0",
        identity_shape="discogs_dual_layout",
        identity_matches=True,
        lock_failure="none",
        job_race=False,
    )
    @example(
        mb_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        discogs_id="12856590",
        invalid_id="invalid:provider",
        sentinel=None,
        identity_shape="dual",
        identity_matches=True,
        lock_failure="none",
        job_race=False,
    )
    @given(
        mb_id=MB_RELEASE_IDS,
        discogs_id=DISCOGS_RELEASE_IDS,
        invalid_id=INVALID_SERVER_IDS,
        sentinel=ABSENT_SENTINELS,
        identity_shape=st.sampled_from(IDENTITY_SHAPES),
        identity_matches=st.booleans(),
        lock_failure=st.sampled_from(("none", "import", "release")),
        job_race=st.booleans(),
    )
    def test_ban_rejections_are_zero_mutation(
        self,
        mb_id: str,
        discogs_id: str,
        invalid_id: str,
        sentinel: object | None,
        identity_shape: str,
        identity_matches: bool,
        lock_failure: str,
        job_race: bool,
    ) -> None:
        primary, secondary = _identity_fields(
            identity_shape,
            mb_id=mb_id,
            discogs_id=discogs_id,
            invalid_id=invalid_id,
            sentinel=sentinel,
        )
        authoritative_release = _authoritative_release(
            identity_shape,
            mb_id=mb_id,
            discogs_id=discogs_id,
        )
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=41,
            status="imported",
            mb_release_id=primary,
            discogs_release_id=secondary,
        ))
        beets = FakeBeetsDB()
        _configure_lock_world(
            db,
            request_id=41,
            lock_failure=lock_failure,
            job_race=job_race,
        )
        expected = (
            authoritative_release
            if identity_matches
            else _different_release(mb_id)
        )
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
        should_succeed = (
            authoritative_release is not None
            and identity_matches
            and lock_failure == "none"
            and not job_race
        )
        self.assertEqual(isinstance(result, BanSourceSuccess), should_succeed)

    @example(
        mb_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        discogs_id="12856590",
        invalid_id="invalid:provider",
        sentinel="0",
        album_shape="mb",
        seed_mb_pipeline=True,
        seed_discogs_pipeline=False,
        release_confirmation="same",
        pipeline_confirmation="same",
        lock_failure="none",
        job_race=False,
        purge_pipeline=True,
        file_payloads=[b"mb-track"],
        sidecar=False,
    )
    @example(
        mb_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        discogs_id="12856590",
        invalid_id="invalid:provider",
        sentinel=" 0 ",
        album_shape="sentinel_primary_valid_discogs",
        seed_mb_pipeline=False,
        seed_discogs_pipeline=True,
        release_confirmation="same",
        pipeline_confirmation="same",
        lock_failure="none",
        job_race=False,
        purge_pipeline=False,
        file_payloads=[b"sentinel-track"],
        sidecar=False,
    )
    @example(
        mb_id="bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
        discogs_id="12856590",
        invalid_id="invalid:provider",
        sentinel=None,
        album_shape="discogs",
        seed_mb_pipeline=False,
        seed_discogs_pipeline=True,
        release_confirmation="absent",
        pipeline_confirmation="absent",
        lock_failure="none",
        job_race=False,
        purge_pipeline=False,
        file_payloads=[b"discogs-track"],
        sidecar=True,
    )
    @given(
        mb_id=MB_RELEASE_IDS,
        discogs_id=DISCOGS_RELEASE_IDS,
        invalid_id=INVALID_SERVER_IDS,
        sentinel=ABSENT_SENTINELS,
        album_shape=st.sampled_from(IDENTITY_SHAPES),
        seed_mb_pipeline=st.booleans(),
        seed_discogs_pipeline=st.booleans(),
        release_confirmation=st.sampled_from(("same", "different", "absent")),
        pipeline_confirmation=st.sampled_from(("same", "different", "absent")),
        lock_failure=st.sampled_from(("none", "import", "release")),
        job_race=st.booleans(),
        purge_pipeline=st.booleans(),
        file_payloads=st.lists(st.binary(max_size=32), max_size=3),
        sidecar=st.booleans(),
    )
    def test_library_delete_authority_across_identity_and_filesystem_worlds(
        self,
        mb_id: str,
        discogs_id: str,
        invalid_id: str,
        sentinel: object | None,
        album_shape: str,
        seed_mb_pipeline: bool,
        seed_discogs_pipeline: bool,
        release_confirmation: str,
        pipeline_confirmation: str,
        lock_failure: str,
        job_race: bool,
        purge_pipeline: bool,
        file_payloads: list[bytes],
        sidecar: bool,
    ) -> None:
        db = FakePipelineDB()
        if seed_mb_pipeline:
            db.seed_request(make_request_row(
                id=41, status="imported", mb_release_id=mb_id,
            ))
        if seed_discogs_pipeline:
            db.seed_request(make_request_row(
                id=42,
                status="imported",
                mb_release_id=discogs_id,
                discogs_release_id=discogs_id,
            ))

        mb_albumid, discogs_albumid = _identity_fields(
            album_shape,
            mb_id=mb_id,
            discogs_id=discogs_id,
            invalid_id=invalid_id,
            sentinel=sentinel,
        )
        authoritative_release = _authoritative_release(
            album_shape,
            mb_id=mb_id,
            discogs_id=discogs_id,
        )
        current_pipeline_id = (
            41 if authoritative_release == mb_id and seed_mb_pipeline
            else 42 if (
                authoritative_release == discogs_id
                and seed_discogs_pipeline
            )
            else None
        )
        expected_release = {
            "same": authoritative_release,
            "different": _different_release(mb_id),
            "absent": None,
        }[release_confirmation]
        other_pipeline_id = (
            42 if current_pipeline_id == 41 and seed_discogs_pipeline
            else 41 if current_pipeline_id == 42 and seed_mb_pipeline
            else 999
        )
        expected_pipeline = {
            "same": current_pipeline_id or 999,
            "different": other_pipeline_id,
            "absent": None,
        }[pipeline_confirmation]
        _configure_lock_world(
            db,
            request_id=current_pipeline_id,
            lock_failure=lock_failure,
            job_race=job_race,
        )

        with tempfile.TemporaryDirectory() as raw_root:
            root = Path(raw_root)
            album_dir = root / "Artist" / "Album"
            album_dir.mkdir(parents=True)
            tracks: list[dict[str, object]] = []
            track_paths: list[Path] = []
            for index, payload in enumerate(file_payloads, start=1):
                track_path = album_dir / f"{index:02d}.flac"
                track_path.write_bytes(payload)
                track_paths.append(track_path)
                tracks.append({"id": index, "path": str(track_path)})
            sidecar_path = album_dir / "cover.jpg"
            if sidecar:
                sidecar_path.write_bytes(b"cover")

            beets = FakeBeetsDB()
            beets.set_album_detail(7, {
                "id": 7,
                "album": "A",
                "artist": "B",
                "mb_albumid": mb_albumid,
                "discogs_albumid": discogs_albumid,
                "tracks": tracks,
            })
            before = snapshot_state(
                db,
                beets,
                request_ids=(41, 42),
                album_id=7,
                filesystem_root=root,
            )

            result = delete_release_from_library(
                pipeline_db=db,
                beets_db=beets,
                request=DeleteRequest(
                    album_id=7,
                    purge_pipeline=purge_pipeline,
                    expected_pipeline_id=expected_pipeline,
                    expected_release_id=expected_release,
                ),
            )

            after = snapshot_state(
                db,
                beets,
                request_ids=(41, 42),
                album_id=7,
                filesystem_root=root,
            )
            succeeded = isinstance(result, DeleteSuccess)
            assert_rejection_preserved_state(
                before,
                after,
                rejected=not succeeded,
            )
            confirmation_ok = (
                release_confirmation != "different"
                and (
                    pipeline_confirmation == "absent"
                    or (
                        pipeline_confirmation == "same"
                        and current_pipeline_id is not None
                    )
                )
            )
            locks_ok = (
                lock_failure != "release"
                and not (
                    current_pipeline_id is not None
                    and lock_failure == "import"
                )
            )
            job_ok = current_pipeline_id is None or not job_race
            should_succeed = (
                authoritative_release is not None
                and confirmation_ok
                and locks_ok
                and job_ok
            )
            self.assertEqual(succeeded, should_succeed)
            if succeeded:
                self.assertIsNone(beets.get_album_detail(7))
                self.assertTrue(all(not path.exists() for path in track_paths))
                self.assertEqual(sidecar_path.exists(), sidecar)
                for request_id in (41, 42):
                    should_be_deleted = (
                        purge_pipeline and request_id == current_pipeline_id
                    )
                    self.assertEqual(
                        db.get_request(request_id) is None,
                        should_be_deleted or (
                            request_id == 41 and not seed_mb_pipeline
                        ) or (
                            request_id == 42 and not seed_discogs_pipeline
                        ),
                    )


class TestDestructiveAuthorityCheckerKnownBad(unittest.TestCase):
    def test_checker_trips_on_fault_injected_production_mutation(self) -> None:
        class FaultInjectingPipelineDB(FakePipelineDB):
            inject_mutation = False

            def get_request_by_release_id(
                self, release_id: object | None,
            ) -> dict[str, object] | None:
                row = super().get_request_by_release_id(release_id)
                if self.inject_mutation:
                    self.inject_mutation = False
                    self.denylist.append(DenylistEntry(
                        request_id=41,
                        username="planted production mutation",
                    ))
                return row

        db = FaultInjectingPipelineDB()
        db.seed_request(make_request_row(
            id=41, status="imported", mb_release_id="12856590",
            discogs_release_id="12856590",
        ))
        beets = FakeBeetsDB()
        beets.set_album_detail(7, {
            "id": 7,
            "album": "A",
            "artist": "B",
            "mb_albumid": "12856590",
            "discogs_albumid": "12856590",
            "tracks": [],
        })
        before = snapshot_state(db, beets, request_ids=(41,), album_id=7)
        db.inject_mutation = True

        result = delete_release_from_library(
            pipeline_db=db,
            beets_db=beets,
            request=DeleteRequest(album_id=7, expected_pipeline_id=999),
        )

        self.assertNotIsInstance(result, DeleteSuccess)
        after = snapshot_state(db, beets, request_ids=(41,), album_id=7)
        with self.assertRaisesRegex(AssertionError, "mutated owned state"):
            assert_rejection_preserved_state(before, after, rejected=True)


if __name__ == "__main__":
    unittest.main()
