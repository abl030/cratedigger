"""One non-blocking lock scope for request-association writers (#1070)."""

from __future__ import annotations

from collections.abc import Callable, Generator, Iterable
from contextlib import AbstractContextManager, ExitStack, contextmanager
from dataclasses import dataclass
from typing import Protocol

from lib.pipeline_db import ADVISORY_LOCK_NAMESPACE_RELEASE, release_id_to_lock_key
from lib.release_identity import ReleaseIdentity


class ReleaseAssociationLockDB(Protocol):
    """The small shared DB surface for RELEASE association locking."""

    def advisory_lock(
        self, namespace: int, key: int,
    ) -> AbstractContextManager[bool]: ...


@dataclass(frozen=True)
class ReleaseIdentityLockResult:
    """The typed result of attempting all affected RELEASE locks."""

    acquired: bool
    keys: tuple[int, ...]


ReleaseLockKeyFn = Callable[[ReleaseIdentity], int]


@contextmanager
def release_identity_locks(
    db: ReleaseAssociationLockDB,
    identities: Iterable[ReleaseIdentity],
    *,
    lock_key_fn: ReleaseLockKeyFn = lambda identity: release_id_to_lock_key(
        identity.release_id,
    ),
) -> Generator[ReleaseIdentityLockResult]:
    """Acquire a deduplicated RELEASE-key set in deterministic order.

    Request-backed callers take their IMPORT lock before entering this scope.
    A later non-blocking contention unwinds every earlier acquisition before
    returning a typed not-acquired result. Key rather than identity
    deduplication deliberately treats a CRC collision as one safe, broader
    serialization boundary.
    """
    keys = tuple(sorted({lock_key_fn(identity) for identity in identities}))
    with ExitStack() as stack:
        for key in keys:
            acquired = stack.enter_context(
                db.advisory_lock(ADVISORY_LOCK_NAMESPACE_RELEASE, key),
            )
            if not acquired:
                yield ReleaseIdentityLockResult(acquired=False, keys=keys)
                return
        yield ReleaseIdentityLockResult(acquired=True, keys=keys)


__all__ = [
    "ReleaseAssociationLockDB",
    "ReleaseIdentityLockResult",
    "ReleaseLockKeyFn",
    "release_identity_locks",
]
