#!/usr/bin/env python3
"""Generated no-mutation laws for destructive release authority."""

from __future__ import annotations

import copy
from collections.abc import Mapping
import logging
import os
import subprocess as sp
import tempfile
import unittest
import uuid
from dataclasses import dataclass
from pathlib import Path
from unittest.mock import MagicMock, patch

from hypothesis import example, given, strategies as st
import msgspec

import tests._hypothesis_profiles  # noqa: F401
from lib.destructive_release_service import (
    BanSourceBeetsAmbiguous,
    BanSourceCleanupIncomplete,
    BanSourceSuccess,
    BanSourceRequest,
    DeleteIncomplete,
    DeleteBeetsAmbiguous,
    DeleteRequest,
    DeleteSuccess,
    ban_source,
    delete_release_from_library,
)
from lib.beets_delete import (
    BeetsDeleteCompleted,
    BeetsDeleteFailureReason,
    BeetsDeleteRequest,
    run_beets_delete,
)
from lib.beets_delete import (
    BeetsDeleteFailed,
    _OwnedPath,
    _configuration_matches,
    _delete_manifest,
)
from lib.import_queue import IMPORT_JOB_AUTOMATION
from lib.config import CratediggerConfig
from lib.mbid_replace_service import (
    MbidReplaceService,
    REPLACE_REASON_CURRENT_BEETS_AMBIGUOUS,
    REPLACE_REASON_SOURCE_IDENTITY_INVALID,
    RESULT_REPLACED,
    RESULT_WRONG_STATE,
)
from lib.pipeline_db import (
    ADVISORY_LOCK_NAMESPACE_IMPORT,
    ADVISORY_LOCK_NAMESPACE_RELEASE,
    AlbumRequestRow,
)
from lib.release_identity import ReleaseIdentity
from tests.fakes import DenylistEntry, FakeBeetsDB, FakePipelineDB
from tests.helpers import make_request_row


def assert_fresh_destructive_authority(
    *,
    authority: str,
    beets_delete_album_ids: tuple[int, ...],
    expected_album_id: int | None,
    destructive_result: bool,
) -> None:
    """Only one fresh unique snapshot can authorize a Beets mutation."""
    if authority == "unique":
        if beets_delete_album_ids != (expected_album_id,):
            raise AssertionError("unique authority did not target its exact album id")
        return
    if beets_delete_album_ids:
        raise AssertionError(f"{authority} authority reached a Beets mutation")
    if authority == "ambiguous" and destructive_result:
        raise AssertionError("ambiguous authority reported destructive success")


@dataclass(frozen=True)
class DestructiveState:
    requests: tuple[tuple[int, Mapping[str, object] | None], ...]
    denylist: tuple[object, ...]
    hashes: tuple[object, ...]
    logs: tuple[object, ...]
    album: dict[str, object] | None
    files: tuple[tuple[str, bytes], ...]
    directories: tuple[str, ...]


def assert_ban_completion_truth(*, completed: bool, absent_after: bool) -> None:
    if completed != absent_after:
        raise AssertionError("ban completion did not match exact-release absence")


def assert_protocol_truth(*, accepted: bool, canonical: bool) -> None:
    if accepted != canonical:
        raise AssertionError("delete protocol accepted a non-canonical frame")


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


def assert_ban_searchability_preserved(
    *, initial_status: str, final_status: str,
) -> None:
    """Bad Rip changes source authority, not operator searchability."""
    expected = "unsearchable" if initial_status == "unsearchable" else "wanted"
    if final_status != expected:
        raise AssertionError(
            f"bad rip changed searchability: {initial_status!r} -> "
            f"{final_status!r}; expected {expected!r}"
        )


def assert_delete_postcondition(
    *,
    outcome: str,
    owned_paths_present: bool,
    unknown_bytes_preserved: bool,
    beets_album_present: bool,
    pipeline_present: bool,
) -> None:
    """Executable law for cleanup success, retryable failure, and PG partial."""
    if not unknown_bytes_preserved:
        raise AssertionError("unknown content was deleted or changed")
    if outcome == "success":
        if owned_paths_present or beets_album_present or pipeline_present:
            raise AssertionError("success postcondition is incomplete")
    elif outcome == "cleanup_failure":
        if not beets_album_present or not pipeline_present:
            raise AssertionError("cleanup failure lost retry authority")
    elif outcome == "pg_partial":
        if owned_paths_present or beets_album_present or not pipeline_present:
            raise AssertionError("PG partial state is not explicit")
    else:
        raise AssertionError(f"unknown outcome {outcome}")


def assert_ambiguous_delete_fails_closed(
    *,
    completed: bool,
    pipeline_present: bool,
    notification_count: int,
    context_retained: bool,
) -> None:
    """A synchronous lost acknowledgement always requires manual recovery."""
    if completed:
        raise AssertionError("ambiguous delete acknowledgement was promoted")
    if not pipeline_present:
        raise AssertionError("ambiguous delete purged pipeline authority")
    if notification_count:
        raise AssertionError("ambiguous delete notified media servers")
    if not context_retained:
        raise AssertionError("ambiguous delete lost operator recovery context")


def assert_enumeration_failure_fails_closed(
    *,
    completed: bool,
    beets_present: bool,
    pipeline_present: bool,
    notification_count: int,
) -> None:
    """Unknown-content enumeration failure retains both authorities."""
    if completed:
        raise AssertionError("enumeration failure was reported as success")
    if not beets_present:
        raise AssertionError("enumeration failure removed Beets authority")
    if not pipeline_present:
        raise AssertionError("enumeration failure purged pipeline authority")
    if notification_count:
        raise AssertionError("enumeration failure notified media servers")


def assert_presence_probe_failure_fails_closed(
    *,
    completed: bool,
    beets_present: bool,
    pipeline_present: bool,
    notification_count: int,
) -> None:
    """Presence-probe failure retains both deletion authorities."""
    if completed:
        raise AssertionError("presence-probe failure was reported as success")
    if not beets_present:
        raise AssertionError("presence-probe failure removed Beets authority")
    if not pipeline_present:
        raise AssertionError("presence-probe failure purged pipeline authority")
    if notification_count:
        raise AssertionError("presence-probe failure notified media servers")


def assert_replace_identity_conflict_fails_closed(
    *,
    outcome: str,
    reason: str | None,
    state_preserved: bool,
    authority_boundary_reached: bool,
) -> None:
    """Conflicting source identities are a typed pre-mutation rejection."""
    if outcome != RESULT_WRONG_STATE:
        raise AssertionError("Replace identity conflict was not rejected")
    if reason != REPLACE_REASON_SOURCE_IDENTITY_INVALID:
        raise AssertionError("Replace identity conflict lost its typed reason")
    if not state_preserved:
        raise AssertionError("Replace identity conflict mutated pipeline state")
    if authority_boundary_reached:
        raise AssertionError("Replace identity conflict reached mutation authority")


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
        action="ban", source="musicbrainz", authority="ambiguous",
        album_id=7,
    )
    @example(
        action="delete", source="discogs", authority="unique", album_id=41,
    )
    @given(
        action=st.sampled_from(("ban", "delete", "replace")),
        source=st.sampled_from(("musicbrainz", "discogs")),
        authority=st.sampled_from(("unique", "missing", "ambiguous")),
        album_id=st.integers(min_value=1, max_value=2_000_000_000),
    )
    def test_each_destructive_caller_requires_fresh_unique_album_authority(
        self,
        action: str,
        source: str,
        authority: str,
        album_id: int,
    ) -> None:
        release_id = (
            "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
            if source == "musicbrainz" else "12856590"
        )
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=41,
            status="imported",
            mb_release_id=release_id,
            discogs_release_id=(release_id if source == "discogs" else None),
        ))
        beets = FakeBeetsDB(library_root="/library")
        beets.set_album_detail(album_id, {
            "id": album_id,
            "album": "Album",
            "artist": "Artist",
            "mb_albumid": release_id,
            "discogs_albumid": (
                release_id if source == "discogs" else None
            ),
            "tracks": [],
        })
        if authority == "unique":
            beets.set_album_ids_for_release(release_id, [album_id])
            beets.set_item_paths(
                release_id,
                [(album_id * 10, f"/library/current-{album_id}/01.flac")],
            )
        elif authority == "ambiguous":
            beets.set_album_ids_for_release(
                release_id,
                [album_id, album_id + 2_000_000_001],
            )

        delete_album_ids: list[int] = []

        def exact_delete(request: BeetsDeleteRequest) -> BeetsDeleteCompleted:
            delete_album_ids.append(request.album_id)
            beets._album_detail.pop(request.album_id, None)
            return BeetsDeleteCompleted(
                album_id=request.album_id,
                album_name="Album",
                artist_name="Artist",
                former_album_path=f"/library/current-{request.album_id}",
                deleted_tracks=1,
                deleted_artifacts=1,
                preserved_paths=(),
            )

        if action == "ban":
            with patch(
                "lib.destructive_release_service.hash_audio_content",
                return_value="generated-hash",
            ):
                result = ban_source(
                    pipeline_db=db,
                    beets_db=beets,
                    request=BanSourceRequest(request_id=41),
                    beets_delete_fn=exact_delete,
                )
            destructive_result = isinstance(result, BanSourceSuccess)
            if authority == "ambiguous":
                self.assertIsInstance(result, BanSourceBeetsAmbiguous)
        elif action == "delete":
            result = delete_release_from_library(
                pipeline_db=db,
                beets_db=beets,
                request=DeleteRequest(album_id=album_id),
                beets_delete_fn=exact_delete,
                notify_fn=lambda _path: (),
            )
            destructive_result = isinstance(result, DeleteSuccess)
            if authority == "ambiguous":
                self.assertIsInstance(result, DeleteBeetsAmbiguous)
        else:
            target_release_id = (
                "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
                if source == "musicbrainz" else "12856591"
            )
            target_group_id = (
                "11111111-1111-1111-1111-111111111111"
                if source == "musicbrainz" else "7654321"
            )
            db.request(41)["mb_release_group_id"] = target_group_id
            scans: list[str | None] = []

            target = {
                "id": target_release_id,
                "title": "Replacement",
                "artist_name": "Artist",
                "artist_id": "artist-1",
                "release_group_id": target_group_id,
                "year": 2026,
                "country": "AU",
                "tracks": [
                    {"disc_number": 1, "track_number": 1, "title": "One"},
                ],
            }
            service = MbidReplaceService(
                db=db,
                config=CratediggerConfig(),
                beets_db_factory=lambda: beets,
                mb_lookup=lambda _rid, *, fresh=False: target,
                discogs_lookup=lambda _rid, *, fresh=False: target,
                search_plan_service=MagicMock(),
                beets_delete_fn=exact_delete,
            )
            before = db.request(41).copy()
            with (
                patch(
                    "lib.mbid_replace_service.trigger_plex_scan",
                    side_effect=lambda _cfg, imported_path=None: scans.append(
                        imported_path,
                    ),
                ),
                patch(
                    "lib.mbid_replace_service.trigger_jellyfin_scan",
                    side_effect=lambda _cfg, imported_path=None: scans.append(
                        imported_path,
                    ),
                ),
            ):
                result = service.replace_request_mbid(
                    41,
                    target_mb_release_id=target_release_id,
                )
            destructive_result = result.outcome == RESULT_REPLACED
            if authority == "ambiguous":
                self.assertEqual(
                    result.reason,
                    REPLACE_REASON_CURRENT_BEETS_AMBIGUOUS,
                )
                self.assertEqual(db.request(41), before)
                self.assertEqual(scans, [])
            elif authority == "unique":
                self.assertEqual(
                    scans,
                    [
                        f"/library/current-{album_id}",
                        f"/library/current-{album_id}",
                    ],
                )
            else:
                self.assertEqual(scans, [None, None])

        assert_fresh_destructive_authority(
            authority=authority,
            beets_delete_album_ids=tuple(delete_album_ids),
            expected_album_id=(album_id if authority == "unique" else None),
            destructive_result=destructive_result,
        )

    @example(
        mb_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        discogs_id="12856590",
        status="imported",
    )
    @given(
        mb_id=MB_RELEASE_IDS,
        discogs_id=DISCOGS_RELEASE_IDS,
        status=st.sampled_from(("wanted", "imported", "unsearchable")),
    )
    def test_replace_conflicting_source_identities_are_zero_mutation(
        self,
        mb_id: str,
        discogs_id: str,
        status: str,
    ) -> None:
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=41,
            status=status,
            mb_release_id=mb_id,
            discogs_release_id=discogs_id,
            mb_release_group_id="11111111-1111-1111-1111-111111111111",
        ))
        before = copy.deepcopy(db.get_request(41))
        mb_lookup = MagicMock()
        discogs_lookup = MagicMock()
        beets_factory = MagicMock()
        service = MbidReplaceService(
            db=db,
            config=CratediggerConfig(),
            beets_db_factory=beets_factory,
            mb_lookup=mb_lookup,
            discogs_lookup=discogs_lookup,
            search_plan_service=MagicMock(),
        )
        mb_lookup.reset_mock()
        discogs_lookup.reset_mock()
        beets_factory.reset_mock()

        result = service.replace_request_mbid(
            41,
            target_mb_release_id=_different_release(mb_id),
        )

        after = db.get_request(41)
        assert_replace_identity_conflict_fails_closed(
            outcome=result.outcome,
            reason=result.reason,
            state_preserved=before == after,
            authority_boundary_reached=bool(
                db.advisory_lock_calls
                or mb_lookup.mock_calls
                or discogs_lookup.mock_calls
                or beets_factory.mock_calls
            ),
        )

    @example(
        initial_status="imported", authority="unique", delete_succeeds=False,
    )
    @example(
        initial_status="unsearchable", authority="missing",
        delete_succeeds=True,
    )
    @given(
        initial_status=st.sampled_from(("wanted", "imported", "unsearchable")),
        authority=st.sampled_from(("unique", "missing")),
        delete_succeeds=st.booleans(),
    )
    def test_ban_completion_is_exactly_authoritative_absence(
        self,
        initial_status: str,
        authority: str,
        delete_succeeds: bool,
    ) -> None:
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=41,
            status=initial_status,
            mb_release_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        ))
        beets = FakeBeetsDB()
        if authority == "unique":
            beets.set_album_ids_for_release(
                "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                [7],
            )

        def exact_delete(
            request: BeetsDeleteRequest,
        ) -> BeetsDeleteCompleted | BeetsDeleteFailed:
            if delete_succeeds:
                return BeetsDeleteCompleted(
                    album_id=request.album_id,
                    album_name="Album",
                    artist_name="Artist",
                    former_album_path="/tmp/fake-beets-library/album-7",
                    deleted_tracks=1,
                    deleted_artifacts=1,
                    preserved_paths=(),
                )
            return BeetsDeleteFailed(
                album_id=request.album_id,
                reason="filesystem_error",
                detail="generated exact-delete failure",
                album_still_present=True,
            )

        result = ban_source(
            pipeline_db=db,
            beets_db=beets,
            request=BanSourceRequest(41),
            beets_delete_fn=exact_delete,
        )

        completed = isinstance(result, BanSourceSuccess)
        absent_after = authority == "missing" or delete_succeeds
        assert_ban_completion_truth(
            completed=completed,
            absent_after=absent_after,
        )
        self.assertEqual(
            isinstance(result, BanSourceCleanupIncomplete),
            not absent_after,
        )
        assert isinstance(result, (BanSourceSuccess, BanSourceCleanupIncomplete))
        self.assertEqual(db.request(41)["status"], (
            "unsearchable" if initial_status == "unsearchable" else "wanted"
        ))
        self.assertEqual(db.download_logs[-1].outcome, "curator_ban")
        self.assertEqual(
            len(result.cleanup_errors),
            0 if absent_after else 1,
        )

    @given(
        prefix=st.binary(min_size=0, max_size=24),
        suffix=st.binary(min_size=0, max_size=24),
        album_id=st.integers(min_value=1, max_value=2**31 - 1),
    )
    @example(prefix=b"plugin output\n", suffix=b"", album_id=7)
    @example(prefix=b"", suffix=b"\n", album_id=7)
    @example(prefix=b"", suffix=b"{}", album_id=7)
    def test_delete_protocol_accepts_only_one_canonical_frame(
        self,
        prefix: bytes,
        suffix: bytes,
        album_id: int,
    ) -> None:
        outcome = BeetsDeleteFailed(
            album_id=album_id,
            reason="album_not_found",
            detail="generated",
            album_still_present=False,
        )
        canonical = msgspec.json.encode(outcome)
        raw = prefix + canonical + suffix

        previous_disable = logging.root.manager.disable
        logging.disable(logging.CRITICAL)
        try:
            result = run_beets_delete(
                BeetsDeleteRequest(
                    album_id=album_id,
                    expected_release_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                    library_db_path="/tmp/library.db",
                    library_root="/tmp/library",
                ),
                runner=lambda argv, **_kwargs: sp.CompletedProcess(
                    argv, 0, stdout=raw, stderr=b"",
                ),
            )
        finally:
            logging.disable(previous_disable)

        accepted = result == outcome
        assert_protocol_truth(
            accepted=accepted,
            canonical=not prefix and not suffix,
        )
        if prefix or suffix:
            self.assertIsInstance(result, BeetsDeleteFailed)
            assert isinstance(result, BeetsDeleteFailed)
            self.assertEqual(result.reason, "protocol_error")

    @given(initial_status=st.sampled_from(("imported", "unsearchable")))
    @example(initial_status="unsearchable")
    def test_successful_ban_preserves_searchability(
        self,
        initial_status: str,
    ) -> None:
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=41,
            status=initial_status,
            mb_release_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        ))
        beets = FakeBeetsDB()

        result = ban_source(
            pipeline_db=db,
            beets_db=beets,
            request=BanSourceRequest(41),
        )

        self.assertIsInstance(result, BanSourceSuccess)
        assert isinstance(result, BanSourceSuccess)
        self.assertEqual(result.request_status, db.request(41)["status"])
        assert_ban_searchability_preserved(
            initial_status=initial_status,
            final_status=str(db.request(41)["status"]),
        )
        self.assertIsNotNone(
            db.request(41).get("priority_started_at"),
            "a successful bad-rip action did not start its priority window",
        )

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
            if authoritative_release is not None:
                beets.set_album_ids_for_release(authoritative_release, [7])
                beets.set_item_paths(
                    authoritative_release,
                    [
                        (int(str(track["id"])), str(track["path"]))
                        for track in tracks
                    ],
                )

            def beets_delete(
                request: BeetsDeleteRequest,
            ) -> BeetsDeleteCompleted:
                deleted = 0
                for path in track_paths:
                    if path.exists():
                        path.unlink()
                        deleted += 1
                if sidecar_path.exists():
                    sidecar_path.unlink()
                    deleted += 1
                beets._album_detail.pop(request.album_id)
                return BeetsDeleteCompleted(
                    album_id=request.album_id,
                    album_name="A",
                    artist_name="B",
                    former_album_path=str(album_dir),
                    deleted_tracks=len(track_paths),
                    deleted_artifacts=deleted,
                    preserved_paths=(),
                )
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
                beets_delete_fn=beets_delete,
                notify_fn=lambda _path: (),
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
                and bool(file_payloads)
                and confirmation_ok
                and locks_ok
                and job_ok
            )
            self.assertEqual(succeeded, should_succeed)
            if succeeded:
                self.assertIsNone(beets.get_album_detail(7))
                self.assertTrue(all(not path.exists() for path in track_paths))
                self.assertFalse(sidecar_path.exists())
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

    @example(
        reason="subprocess_error", album_present=False, orphan_items=False,
        path_source="track", purge_pipeline=True,
    )
    @example(
        reason="protocol_error", album_present=False, orphan_items=True,
        path_source="art", purge_pipeline=True,
    )
    @given(
        reason=st.sampled_from(("subprocess_error", "protocol_error")),
        album_present=st.booleans(),
        orphan_items=st.booleans(),
        path_source=st.sampled_from(("track", "art", "none")),
        purge_pipeline=st.booleans(),
    )
    def test_lost_delete_ack_always_requires_manual_recovery(
        self,
        reason: BeetsDeleteFailureReason,
        album_present: bool,
        orphan_items: bool,
        path_source: str,
        purge_pipeline: bool,
    ) -> None:
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=41,
            status="imported",
            mb_release_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        ))
        album_dir = Path("/music/Artist/Album")
        detail: dict[str, object] = {
            "id": 7,
            "album": "Album",
            "artist": "Artist",
            "mb_albumid": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "discogs_albumid": None,
            "tracks": [],
        }
        if path_source == "track":
            detail["tracks"] = [{"id": 1, "path": str(album_dir / "01.flac")}]
        elif path_source == "art":
            detail["artpath"] = str(album_dir / "cover.jpg")
        beets = FakeBeetsDB()
        beets.set_album_detail(7, detail)
        beets.set_album_ids_for_release(
            "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            [7],
        )
        beets.set_item_paths(
            "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            [(1, str(album_dir / "01.flac"))],
        )
        notifications: list[str] = []

        def failed_child(request: BeetsDeleteRequest) -> BeetsDeleteFailed:
            if not album_present:
                beets._album_detail.pop(request.album_id)
            beets.set_orphan_items_present(request.album_id, orphan_items)
            return BeetsDeleteFailed(
                album_id=request.album_id,
                reason=reason,
                detail="generated child boundary failure",
                album_still_present=album_present,
            )

        previous_disable = logging.root.manager.disable
        logging.disable(logging.CRITICAL)
        try:
            result = delete_release_from_library(
                pipeline_db=db,
                beets_db=beets,
                request=DeleteRequest(
                    album_id=7,
                    purge_pipeline=purge_pipeline,
                ),
                beets_delete_fn=failed_child,
                notify_fn=lambda path: notifications.append(path) or (),
            )
        finally:
            logging.disable(previous_disable)

        # The fresh joined resolver sees the current item path even when the
        # earlier album-detail projection had no recovery path at all.
        expected_path = str(album_dir)
        context_retained = (
            isinstance(result, DeleteIncomplete)
            and result.album_name == "Album"
            and result.artist_name == "Artist"
            and result.former_album_path == expected_path
            and result.pipeline_request_id == 41
            and result.pipeline_status == "imported"
            and result.acknowledgement_lost
            and result.deleted_files is None
            and result.deleted_artifacts is None
            and "metadata may be gone" in result.detail
            and "was preserved" in result.detail
        )
        assert_ambiguous_delete_fails_closed(
            completed=isinstance(result, DeleteSuccess),
            pipeline_present=db.get_request(41) is not None,
            notification_count=len(notifications),
            context_retained=context_retained,
        )

    @example(
        track_presence=[True, True], art_present=True, sidecar_present=True,
        unknown_payload=b"booklet", fault_at=-1, noop=False,
    )
    @example(
        track_presence=[True], art_present=True, sidecar_present=True,
        unknown_payload=None, fault_at=2, noop=False,
    )
    @given(
        track_presence=st.lists(st.booleans(), min_size=1, max_size=4),
        art_present=st.booleans(),
        sidecar_present=st.booleans(),
        unknown_payload=st.one_of(st.none(), st.binary(max_size=32)),
        fault_at=st.integers(min_value=-1, max_value=7),
        noop=st.booleans(),
    )
    def test_beets_delete_manifest_law_across_partial_worlds(
        self,
        track_presence: list[bool],
        art_present: bool,
        sidecar_present: bool,
        unknown_payload: bytes | None,
        fault_at: int,
        noop: bool,
    ) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            targets: list[_OwnedPath] = []
            for index, present in enumerate(track_presence):
                path = root / f"{index:02d}.flac"
                if present:
                    path.write_bytes(bytes([index]))
                targets.append(_OwnedPath(str(path), "track"))
            art = root / "cover.jpg"
            if art_present:
                art.write_bytes(b"art")
            targets.append(_OwnedPath(str(art), "art"))
            sidecar = root / "cratedigger.json"
            if sidecar_present:
                sidecar.write_bytes(b"sidecar")
            targets.append(_OwnedPath(str(sidecar), "sidecar"))
            unknown = root / "booklet.pdf"
            if unknown_payload is not None:
                unknown.write_bytes(unknown_payload)

            metadata_present = True
            remove_calls = 0

            def remove(path: str) -> None:
                nonlocal remove_calls
                call = remove_calls
                remove_calls += 1
                if call == fault_at:
                    raise OSError("generated fault")
                if noop:
                    return
                if os.path.lexists(path):
                    os.remove(path)

            def remove_metadata() -> None:
                nonlocal metadata_present
                metadata_present = False

            outcome = _delete_manifest(
                album_id=7, album_name="Album", artist_name="Artist",
                owned_paths=tuple(targets), album_dirs=(str(root),),
                metadata_remove=remove_metadata,
                album_present=lambda: metadata_present,
                remove_path=remove, prune_dir=lambda _path: None,
            )

            unknown_ok = (
                unknown_payload is None
                or (unknown.exists() and unknown.read_bytes() == unknown_payload)
            )
            if isinstance(outcome, BeetsDeleteCompleted):
                assert_delete_postcondition(
                    outcome="success",
                    owned_paths_present=any(
                        os.path.lexists(item.path) for item in targets),
                    unknown_bytes_preserved=unknown_ok,
                    beets_album_present=metadata_present,
                    pipeline_present=False,
                )
            else:
                self.assertIsInstance(outcome, BeetsDeleteFailed)
                assert_delete_postcondition(
                    outcome="cleanup_failure",
                    owned_paths_present=any(
                        os.path.lexists(item.path) for item in targets),
                    unknown_bytes_preserved=unknown_ok,
                    beets_album_present=metadata_present,
                    pipeline_present=True,
                )

    @example(fault_call=1, unknown_payload=b"booklet")
    @example(fault_call=2, unknown_payload=None)
    @given(
        fault_call=st.sampled_from((1, 2)),
        unknown_payload=st.one_of(st.none(), st.binary(max_size=32)),
    )
    def test_unknown_enumeration_failure_retains_beets_pg_and_notifications(
        self,
        fault_call: int,
        unknown_payload: bytes | None,
    ) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            track = root / "01.flac"
            track.write_bytes(b"audio")
            unknown = root / "booklet.pdf"
            if unknown_payload is not None:
                unknown.write_bytes(unknown_payload)

            db = FakePipelineDB()
            db.seed_request(make_request_row(
                id=41,
                status="imported",
                mb_release_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            ))
            beets = FakeBeetsDB()
            beets.set_album_detail(7, {
                "id": 7,
                "album": "Album",
                "artist": "Artist",
                "mb_albumid": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                "discogs_albumid": None,
                "path": str(root),
                "tracks": [{"id": 1, "path": str(track)}],
            })
            beets.set_album_ids_for_release(
                "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                [7],
            )
            beets.set_item_paths(
                "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                [(1, str(track))],
            )
            list_calls = 0
            notifications: list[str] = []

            def list_with_fault(directory: Path) -> tuple[Path, ...]:
                nonlocal list_calls
                list_calls += 1
                if list_calls == fault_call:
                    raise OSError("generated enumeration fault")
                return tuple(directory.iterdir())

            def remove_metadata() -> None:
                beets._album_detail.pop(7)

            def failed_enumeration(
                request: BeetsDeleteRequest,
            ) -> BeetsDeleteCompleted | BeetsDeleteFailed:
                return _delete_manifest(
                    album_id=request.album_id,
                    album_name="Album",
                    artist_name="Artist",
                    owned_paths=(_OwnedPath(str(track), "track"),),
                    album_dirs=(str(root),),
                    metadata_remove=remove_metadata,
                    album_present=lambda: (
                        beets.get_album_detail(request.album_id) is not None
                    ),
                    remove_path=lambda path: os.remove(path),
                    prune_dir=lambda _path: None,
                    list_dir=list_with_fault,
                )

            result = delete_release_from_library(
                pipeline_db=db,
                beets_db=beets,
                request=DeleteRequest(album_id=7, purge_pipeline=True),
                beets_delete_fn=failed_enumeration,
                notify_fn=lambda path: notifications.append(path) or (),
            )

            assert_enumeration_failure_fails_closed(
                completed=isinstance(result, DeleteSuccess),
                beets_present=beets.get_album_detail(7) is not None,
                pipeline_present=db.get_request(41) is not None,
                notification_count=len(notifications),
            )
            self.assertIsInstance(result, DeleteIncomplete)
            assert isinstance(result, DeleteIncomplete)
            self.assertEqual(result.reason, "filesystem_error")
            if unknown_payload is not None:
                self.assertEqual(unknown.read_bytes(), unknown_payload)

    @example(fault_stage="pre", purge_pipeline=True)
    @example(fault_stage="post", purge_pipeline=True)
    @example(fault_stage="progress", purge_pipeline=False)
    @example(fault_stage="final", purge_pipeline=True)
    @given(
        fault_stage=st.sampled_from(("pre", "post", "progress", "final")),
        purge_pipeline=st.booleans(),
    )
    def test_presence_probe_faults_retain_beets_pg_and_notifications(
        self,
        fault_stage: str,
        purge_pipeline: bool,
    ) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            track = root / "01.flac"
            track.write_bytes(b"audio")
            db = FakePipelineDB()
            db.seed_request(make_request_row(
                id=41,
                status="imported",
                mb_release_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            ))
            beets = FakeBeetsDB()
            beets.set_album_detail(7, {
                "id": 7,
                "album": "Album",
                "artist": "Artist",
                "mb_albumid": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                "discogs_albumid": None,
                "path": str(root),
                "tracks": [{"id": 1, "path": str(track)}],
            })
            beets.set_album_ids_for_release(
                "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                [7],
            )
            beets.set_item_paths(
                "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                [(1, str(track))],
            )
            notifications: list[str] = []
            probe_calls = 0
            fault_call = {
                "pre": 1,
                "post": 2,
                "progress": 2,
                "final": 3,
            }[fault_stage]

            def probe(path: str) -> bool:
                nonlocal probe_calls
                probe_calls += 1
                if probe_calls == fault_call:
                    raise OSError(
                        f"generated {fault_stage} presence-probe fault",
                    )
                try:
                    os.lstat(path)
                except FileNotFoundError:
                    return False
                return True

            def remove(path: str) -> None:
                if fault_stage == "progress":
                    raise OSError("generated removal fault")
                os.remove(path)

            def remove_metadata() -> None:
                beets._album_detail.pop(7)

            def failed_probe(
                request: BeetsDeleteRequest,
            ) -> BeetsDeleteCompleted | BeetsDeleteFailed:
                return _delete_manifest(
                    album_id=request.album_id,
                    album_name="Album",
                    artist_name="Artist",
                    owned_paths=(_OwnedPath(str(track), "track"),),
                    album_dirs=(str(root),),
                    metadata_remove=remove_metadata,
                    album_present=lambda: (
                        beets.get_album_detail(request.album_id) is not None
                    ),
                    remove_path=remove,
                    prune_dir=lambda _path: None,
                    path_exists=probe,
                )

            result = delete_release_from_library(
                pipeline_db=db,
                beets_db=beets,
                request=DeleteRequest(
                    album_id=7,
                    purge_pipeline=purge_pipeline,
                ),
                beets_delete_fn=failed_probe,
                notify_fn=lambda path: notifications.append(path) or (),
            )

            assert_presence_probe_failure_fails_closed(
                completed=isinstance(result, DeleteSuccess),
                beets_present=beets.get_album_detail(7) is not None,
                pipeline_present=db.get_request(41) is not None,
                notification_count=len(notifications),
            )
            self.assertIsInstance(result, DeleteIncomplete)
            assert isinstance(result, DeleteIncomplete)
            self.assertIn("presence", result.detail)
            self.assertEqual(result.remaining_owned_paths, (str(track),))

    @given(
        mismatch_db=st.booleans(),
        mismatch_root=st.booleans(),
    )
    @example(mismatch_db=True, mismatch_root=False)
    @example(mismatch_db=False, mismatch_root=True)
    def test_delete_configuration_authority_requires_both_exact_paths(
        self,
        mismatch_db: bool,
        mismatch_root: bool,
    ) -> None:
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            configured_db = base / "configured.db"
            configured_db.touch()
            configured_root = base / "configured-root"
            configured_root.mkdir()
            other_db = base / "other.db"
            other_db.touch()
            other_root = base / "other-root"
            other_root.mkdir()
            request = BeetsDeleteRequest(
                album_id=7,
                expected_release_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                library_db_path=str(other_db if mismatch_db else configured_db),
                library_root=str(other_root if mismatch_root else configured_root),
            )

            authorized = _configuration_matches(
                request, str(configured_db), str(configured_root),
            )

            self.assertEqual(authorized, not (mismatch_db or mismatch_root))


class TestDestructiveAuthorityCheckerKnownBad(unittest.TestCase):
    def test_replace_conflict_checker_kills_each_fail_open_mutant(self) -> None:
        mutants: tuple[tuple[str, str | None, bool, bool], ...] = (
            (
                RESULT_REPLACED, REPLACE_REASON_SOURCE_IDENTITY_INVALID,
                True, False,
            ),
            (RESULT_WRONG_STATE, None, True, False),
            (
                RESULT_WRONG_STATE, REPLACE_REASON_SOURCE_IDENTITY_INVALID,
                False, False,
            ),
            (
                RESULT_WRONG_STATE, REPLACE_REASON_SOURCE_IDENTITY_INVALID,
                True, True,
            ),
        )
        for outcome, reason, state_preserved, boundary_reached in mutants:
            with self.subTest(
                outcome=outcome,
                reason=reason,
                state_preserved=state_preserved,
                boundary_reached=boundary_reached,
            ), self.assertRaises(AssertionError):
                assert_replace_identity_conflict_fails_closed(
                    outcome=outcome,
                    reason=reason,
                    state_preserved=state_preserved,
                    authority_boundary_reached=boundary_reached,
                )

    def test_fresh_authority_checker_kills_cardinality_and_target_mutants(
        self,
    ) -> None:
        mutants: tuple[
            tuple[str, tuple[int, ...], int | None, bool], ...
        ] = (
            ("ambiguous", (7,), None, False),
            ("missing", (7,), None, False),
            ("unique", (8,), 7, True),
            ("ambiguous", (), None, True),
        )
        for authority, album_ids, expected_album_id, destructive in mutants:
            with self.subTest(
                authority=authority,
                album_ids=album_ids,
            ), self.assertRaises(AssertionError):
                assert_fresh_destructive_authority(
                    authority=authority,
                    beets_delete_album_ids=album_ids,
                    expected_album_id=expected_album_id,
                    destructive_result=destructive,
                )

    def test_ban_searchability_checker_kills_resume_mutant(self) -> None:
        with self.assertRaisesRegex(AssertionError, "changed searchability"):
            assert_ban_searchability_preserved(
                initial_status="unsearchable",
                final_status="wanted",
            )

    def test_checker_trips_on_fault_injected_production_mutation(self) -> None:
        class FaultInjectingPipelineDB(FakePipelineDB):
            inject_mutation = False

            def get_request_by_release_id(
                self, release_id: object | None,
            ) -> AlbumRequestRow | None:
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

    def test_delete_checker_kills_each_contract_mutant(self) -> None:
        mutants = {
            "omitted_art": dict(
                outcome="success", owned_paths_present=True,
                unknown_bytes_preserved=True, beets_album_present=False,
                pipeline_present=False),
            "omitted_sidecar": dict(
                outcome="success", owned_paths_present=True,
                unknown_bytes_preserved=True, beets_album_present=False,
                pipeline_present=False),
            "noop_success": dict(
                outcome="success", owned_paths_present=True,
                unknown_bytes_preserved=True, beets_album_present=False,
                pipeline_present=False),
            "unknown_overdelete": dict(
                outcome="success", owned_paths_present=False,
                unknown_bytes_preserved=False, beets_album_present=False,
                pipeline_present=False),
            "early_beets_delete": dict(
                outcome="cleanup_failure", owned_paths_present=True,
                unknown_bytes_preserved=True, beets_album_present=False,
                pipeline_present=True),
            "early_pg_delete": dict(
                outcome="cleanup_failure", owned_paths_present=True,
                unknown_bytes_preserved=True, beets_album_present=True,
                pipeline_present=False),
        }
        for name, world in mutants.items():
            with self.subTest(mutant=name), self.assertRaises(AssertionError):
                assert_delete_postcondition(
                    outcome=str(world["outcome"]),
                    owned_paths_present=bool(world["owned_paths_present"]),
                    unknown_bytes_preserved=bool(world["unknown_bytes_preserved"]),
                    beets_album_present=bool(world["beets_album_present"]),
                    pipeline_present=bool(world["pipeline_present"]),
                )

    def test_ack_checker_kills_each_fail_closed_mutant(self) -> None:
        mutants = {
            "metadata_absence_promoted": dict(
                completed=True, pipeline_present=True,
                notification_count=0, context_retained=True,
            ),
            "pipeline_purged": dict(
                completed=False, pipeline_present=False,
                notification_count=0, context_retained=True,
            ),
            "media_notified": dict(
                completed=False, pipeline_present=True,
                notification_count=1, context_retained=True,
            ),
            "operator_context_lost": dict(
                completed=False, pipeline_present=True,
                notification_count=0, context_retained=False,
            ),
        }
        for name, world in mutants.items():
            with self.subTest(mutant=name), self.assertRaises(AssertionError):
                assert_ambiguous_delete_fails_closed(
                    completed=bool(world["completed"]),
                    pipeline_present=bool(world["pipeline_present"]),
                    notification_count=int(world["notification_count"]),
                    context_retained=bool(world["context_retained"]),
                )

    def test_ban_and_protocol_checkers_kill_historical_mutants(self) -> None:
        with self.assertRaisesRegex(AssertionError, "exact-release absence"):
            assert_ban_completion_truth(completed=True, absent_after=False)
        with self.assertRaisesRegex(AssertionError, "non-canonical frame"):
            assert_protocol_truth(accepted=True, canonical=False)

    def test_enumeration_checker_kills_each_fail_closed_mutant(self) -> None:
        mutants = {
            "reported_success": dict(
                completed=True, beets_present=True,
                pipeline_present=True, notification_count=0,
            ),
            "beets_removed": dict(
                completed=False, beets_present=False,
                pipeline_present=True, notification_count=0,
            ),
            "pipeline_purged": dict(
                completed=False, beets_present=True,
                pipeline_present=False, notification_count=0,
            ),
            "media_notified": dict(
                completed=False, beets_present=True,
                pipeline_present=True, notification_count=1,
            ),
        }
        for name, world in mutants.items():
            with self.subTest(mutant=name), self.assertRaises(AssertionError):
                assert_enumeration_failure_fails_closed(
                    completed=bool(world["completed"]),
                    beets_present=bool(world["beets_present"]),
                    pipeline_present=bool(world["pipeline_present"]),
                    notification_count=int(world["notification_count"]),
                )

    def test_presence_probe_checker_kills_each_fail_closed_mutant(self) -> None:
        mutants = {
            "reported_success": dict(
                completed=True, beets_present=True,
                pipeline_present=True, notification_count=0,
            ),
            "beets_removed": dict(
                completed=False, beets_present=False,
                pipeline_present=True, notification_count=0,
            ),
            "pipeline_purged": dict(
                completed=False, beets_present=True,
                pipeline_present=False, notification_count=0,
            ),
            "media_notified": dict(
                completed=False, beets_present=True,
                pipeline_present=True, notification_count=1,
            ),
        }
        for name, world in mutants.items():
            with self.subTest(mutant=name), self.assertRaises(AssertionError):
                assert_presence_probe_failure_fails_closed(
                    completed=bool(world["completed"]),
                    beets_present=bool(world["beets_present"]),
                    pipeline_present=bool(world["pipeline_present"]),
                    notification_count=int(world["notification_count"]),
                )


if __name__ == "__main__":
    unittest.main()
