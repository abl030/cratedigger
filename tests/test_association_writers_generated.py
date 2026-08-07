"""Generated before/after RELEASE-lock property for association writers (#1070)."""

from __future__ import annotations

import unittest
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from unittest.mock import MagicMock

from hypothesis import given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401
from lib.canonical_release_service import CanonicalReleaseService
from lib.config import CratediggerConfig
from lib.mbid_replace_service import MbidReplaceService
from lib.pipeline_db import ADVISORY_LOCK_NAMESPACE_RELEASE, release_id_to_lock_key
from lib.pipeline_delete_service import delete_pipeline_request
from lib.release_association_locks import release_identity_locks
from lib.release_identity import ReleaseIdentity
from lib.request_identity import acceptable_identities
from lib.wrong_match_delete_service import WrongMatchDeleteSummary
from tests.fakes import FakeBeetsDB, FakePipelineDB, FakeSlskdAPI
from tests.helpers import make_request_row

LOSER = "4878ee47-f8b8-45c8-832c-62de3bccfa6e"
FIRST = "7aabf975-9a06-4b2e-854c-2c700380ebd5"
SECOND = "abe18a1c-ad01-423c-b6ca-63cfa8a9daf1"
TARGET = "bce7d8c3-815b-449c-8e18-df806398986c"
NOW = datetime(2026, 8, 7, tzinfo=UTC)


def check_association_writer_locks(
    before: tuple[str, ...],
    after: tuple[str, ...],
    calls: list[tuple[int, int]],
    *,
    lock_key: Callable[[str], int] = release_id_to_lock_key,
) -> None:
    """Every before/after association key must be protected exactly once."""
    expected = {lock_key(identity) for identity in (*before, *after)}
    actual = [key for namespace, key in calls if namespace == ADVISORY_LOCK_NAMESPACE_RELEASE]
    if actual != sorted(expected):
        raise AssertionError(f"association locks {actual} did not cover {sorted(expected)}")


def _identities(row: Mapping[str, object] | None) -> tuple[str, ...]:
    if row is None or row.get("status") == "replaced":
        return ()
    return tuple(identity.release_id for identity in acceptable_identities(row))


def _empty_wrong_match(_db: object, request_id: int) -> WrongMatchDeleteSummary:
    return WrongMatchDeleteSummary(
        request_id=request_id, outcome="group_empty", success=True,
        processed=0, deleted=0, deleted_paths=0, cleared=0, skipped=0,
        errors=0, remaining=0, group_empty=True, results=(),
    )


class TestAssociationWritersGenerated(unittest.TestCase):
    @given(
        before=st.lists(st.integers(min_value=1, max_value=10_000), max_size=8),
        after=st.lists(st.integers(min_value=1, max_value=10_000), max_size=8),
        collision_modulus=st.integers(min_value=1, max_value=5),
    )
    def test_generated_before_after_sets_dedupe_and_sort_collision_keys(
        self,
        before: list[int],
        after: list[int],
        collision_modulus: int,
    ) -> None:
        """The shared scope protects generated association unions exactly.

        A deliberately tiny key space exercises both duplicate identities and
        CRC-style key collisions without manufacturing invalid dual identities.
        """
        identities = tuple(
            ReleaseIdentity("discogs", str(value))
            for value in (*before, *after)
        )
        db = FakePipelineDB()
        key = lambda release_id: int(release_id) % collision_modulus

        with release_identity_locks(
            db,
            identities,
            lock_key_fn=lambda identity: key(identity.release_id),
        ) as result:
            self.assertTrue(result.acquired)
            self.assertEqual(
                result.keys,
                tuple(sorted({key(identity.release_id) for identity in identities})),
            )

        check_association_writer_locks(
            tuple(str(value) for value in before),
            tuple(str(value) for value in after),
            db.advisory_lock_calls,
            lock_key=key,
        )

    @given(
        old=st.integers(min_value=1, max_value=10_000),
        new=st.integers(min_value=10_001, max_value=20_000),
    )
    def test_generated_omitted_before_or_after_mutants_die(
        self, old: int, new: int,
    ) -> None:
        """Removing either side of a changing association union is unsafe."""
        before = (str(old),)
        after = (str(new),)
        key = int
        for mutant in ((new,), (old,)):
            with self.assertRaises(AssertionError):
                check_association_writer_locks(
                    before,
                    after,
                    [
                        (ADVISORY_LOCK_NAMESPACE_RELEASE, release_id)
                        for release_id in mutant
                    ],
                    lock_key=key,
                )

    @given(st.sampled_from(("add", "move", "retire", "replace", "delete")))
    def test_every_writer_locks_before_and_after_associations(
        self, operation: str,
    ) -> None:
        db = FakePipelineDB()
        request_id = 41
        db.seed_request(make_request_row(
            id=request_id, status="wanted", mb_release_id=LOSER,
            mb_release_group_id="11111111-1111-1111-1111-111111111111",
        ))
        if operation in {"move", "retire", "replace"}:
            db.record_canonical_release_id(
                request_id, canonical_release_id=FIRST, resolved_at=NOW,
            )
        before = _identities(db.get_request(request_id))
        db.advisory_lock_calls.clear()

        if operation == "add":
            CanonicalReleaseService(
                db, canonical_fn=lambda _id: FIRST, now_fn=lambda: NOW,
            ).reconcile_request(request_id)
            after = _identities(db.get_request(request_id))
        elif operation == "move":
            CanonicalReleaseService(
                db, canonical_fn=lambda _id: SECOND, now_fn=lambda: NOW,
            ).reconcile_request(request_id)
            after = _identities(db.get_request(request_id))
        elif operation == "retire":
            CanonicalReleaseService(db).retire_request(request_id)
            after = _identities(db.get_request(request_id))
        elif operation == "delete":
            delete_pipeline_request(db, request_id)
            after = ()
        else:
            plans = MagicMock()
            service = MbidReplaceService(
                db=db, config=CratediggerConfig(), slskd=FakeSlskdAPI(),
                beets_db_factory=FakeBeetsDB,
                mb_lookup=lambda _id, *, fresh: {
                    "id": TARGET, "release_group_id": "11111111-1111-1111-1111-111111111111",
                    "artist_name": "Artist", "artist_id": "artist", "title": "Target",
                    "year": 2026, "country": "AU", "tracks": [],
                },
                search_plan_service=plans,
                wrong_match_delete_fn=_empty_wrong_match,
            )
            service.replace_request_mbid(request_id, target_mb_release_id=TARGET)
            descendant = db.get_request_by_replaces_request_id(request_id)
            after = _identities(descendant)

        check_association_writer_locks(before, after, db.advisory_lock_calls)

    def test_known_bad_omitting_old_identity_dies(self) -> None:
        with self.assertRaises(AssertionError):
            check_association_writer_locks(
                (LOSER, FIRST), (SECOND,),
                [(ADVISORY_LOCK_NAMESPACE_RELEASE, release_id_to_lock_key(SECOND))],
            )
