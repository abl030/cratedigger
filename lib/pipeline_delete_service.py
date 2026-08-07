"""Canonical authority boundary for deleting one pipeline request."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

import psycopg2.errors

from lib import transitions
from lib.pipeline_db import ADVISORY_LOCK_NAMESPACE_IMPORT
from lib.release_association_locks import (
    ReleaseAssociationLockDB,
    release_identity_locks,
)
from lib.request_identity import acceptable_identities

if TYPE_CHECKING:
    from lib.pipeline_db.rows import AlbumRequestRow


class PipelineDeleteDB(ReleaseAssociationLockDB, Protocol):

    def get_request(self, request_id: int) -> AlbumRequestRow | None: ...

    def get_request_by_replaces_request_id(
        self,
        replaced_id: int,
    ) -> AlbumRequestRow | None: ...

    def delete_request(self, request_id: int) -> bool: ...


@dataclass(frozen=True)
class PipelineDeleteApplied:
    request_id: int


@dataclass(frozen=True)
class PipelineDeleteNotFound:
    request_id: int


@dataclass(frozen=True)
class PipelineDeleteLockContended:
    request_id: int


@dataclass(frozen=True)
class PipelineDeleteAssociationChanged:
    request_id: int


@dataclass(frozen=True)
class PipelineDeleteConditionalRejected:
    request_id: int


@dataclass(frozen=True)
class PipelineDeleteDescendantConflict:
    request_id: int
    descendant_request_ids: tuple[int, ...]


type PipelineDeleteResult = (
    PipelineDeleteApplied
    | PipelineDeleteNotFound
    | PipelineDeleteLockContended
    | PipelineDeleteAssociationChanged
    | PipelineDeleteConditionalRejected
    | PipelineDeleteDescendantConflict
    | transitions.TransitionConflict
)


def _descendant_ids(
    db: PipelineDeleteDB,
    request_id: int,
) -> tuple[int, ...]:
    descendants: list[int] = []
    cursor = db.get_request_by_replaces_request_id(request_id)
    while cursor is not None:
        descendant_id = int(cursor["id"])
        descendants.append(descendant_id)
        cursor = db.get_request_by_replaces_request_id(descendant_id)
    return tuple(descendants)


def delete_pipeline_request(
    db: PipelineDeleteDB,
    request_id: int,
) -> PipelineDeleteResult:
    """Delete one request after IMPORT ordering and an exact-owner reread.

    ``delete_request`` repeats the owner-null predicate in SQL. The advisory
    lock provides ordinary service ordering; that final predicate preserves
    safety even if a caller violates the lock protocol.
    """
    with db.advisory_lock(
        ADVISORY_LOCK_NAMESPACE_IMPORT,
        request_id,
    ) as acquired:
        if not acquired:
            return PipelineDeleteLockContended(request_id)

        current = db.get_request(request_id)
        if current is None:
            return PipelineDeleteNotFound(request_id)
        processing_locked = transitions.processing_locked_conflict(
            current,
            request_id,
            "deleted",
            expected_status=str(current["status"]),
        )
        if processing_locked is not None:
            return processing_locked

        # IMPORT outer, then every identity the non-replaced row currently
        # claims. See docs/advisory-locks.md. Deletion removes all of those
        # associations, so a Library delete cannot observe a stale inverse
        # set halfway through this operation.
        associations = acceptable_identities(current)
        with release_identity_locks(db, associations) as release_locks:
            if not release_locks.acquired:
                return PipelineDeleteLockContended(request_id)
            confirmed = db.get_request(request_id)
            if confirmed is None:
                return PipelineDeleteNotFound(request_id)
            if acceptable_identities(confirmed) != associations:
                return PipelineDeleteAssociationChanged(request_id)
            processing_locked = transitions.processing_locked_conflict(
                confirmed,
                request_id,
                "deleted",
                expected_status=str(confirmed["status"]),
            )
            if processing_locked is not None:
                return processing_locked
            descendants = _descendant_ids(db, request_id)
            if descendants:
                return PipelineDeleteDescendantConflict(request_id, descendants)

            try:
                deleted = db.delete_request(request_id)
            except psycopg2.errors.ForeignKeyViolation:
                descendants = _descendant_ids(db, request_id)
                if descendants:
                    return PipelineDeleteDescendantConflict(
                        request_id,
                        descendants,
                    )
                raise
            if deleted:
                return PipelineDeleteApplied(request_id)

            refreshed = db.get_request(request_id)
            processing_locked = transitions.processing_locked_conflict(
                refreshed,
                request_id,
                "deleted",
                expected_status=str(confirmed["status"]),
            )
            if processing_locked is not None:
                return processing_locked
            if refreshed is None:
                return PipelineDeleteNotFound(request_id)
            descendants = _descendant_ids(db, request_id)
            if descendants:
                return PipelineDeleteDescendantConflict(request_id, descendants)
            return PipelineDeleteConditionalRejected(request_id)
