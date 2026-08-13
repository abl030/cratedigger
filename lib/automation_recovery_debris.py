"""Recovery-side removal of a killed automation import's own Beets debris.

RCA (issue #1089): a deploy stop can SIGKILL an in-flight automation import
after Beets has already committed the album + item rows to its SQLite
catalog but before any file has moved. ``recover_abandoned_automation_owners``
(``scripts/importer.py``) correctly proves the abandoned owner dead and
returns the request to ``wanted``, but its journaled cleanup then deletes the
processing source tree the killed child's committed album still points at —
upgrading "ghost with live files" to "ghost with absent files" with zero
Beets-side inspection. The job row already carries everything needed to
notice this: ``beets_launch_release_id`` and ``beets_launch_source_path``.

Authority: "yes do the one shot, yeah we should do B realistaclly so we can
gracefulyl recover from whatever, make sure denylists aren't persisted (looks
like the pipeline already cleans itself up there but just thnking about
loud)" — https://github.com/abl030/cratedigger/issues/1089#issuecomment-5277485982

Scope granted: after the abandoned beets child is proven dead, automation
recovery may remove a beets album it can prove is its own crash debris — its
release identity (``mb_albumid``/``discogs_albumid`` — Cratedigger never
adapts between them; both feed the same overloaded
``mb_release_id``/``beets_launch_release_id`` column) equals the job's
``beets_launch_release_id`` AND every one of its item paths lies under the
job's ``beets_launch_source_path`` — via the admitted exact-album delete lane
(``lib.beets_delete``), and it must never write a ``source_denylist`` row.
Any album failing either precondition — including the partially-moved world
where an item has already reached the library root — is surfaced, never
removed.

This module never mutates a filesystem path. The admitted delete lane's
debris mode (``BeetsDeleteRequest.debris_confinement_root``) removes ONLY the
Beets catalog row (``album.remove(delete=False)``), confined to the launch
source path rather than the configured library root, because crash debris
is — by construction — an album whose files never reached the library. The
processing source tree itself is left for the existing journaled cleanup
(``lib.processing_cleanup``) to remove, so this action composes safely
regardless of whether that cleanup runs before, after, or is a resumed
journal from an earlier interrupted recovery attempt.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal, Protocol, Self

import msgspec

from lib.beets_db import BeetsDB, beets_authority_availability_category
from lib.beets_delete import (
    BeetsDeleteCompleted,
    BeetsDeleteOutcome,
    BeetsDeleteRequest,
    _confined_path,
    run_beets_delete,
)
from lib.json_narrow import (
    is_str_object_dict,
    json_list,
)

RecoveryDebrisOutcomeKind = Literal[
    # Nothing to check: this job never authorized a Beets launch, so it
    # cannot have committed any catalog row.
    "no_launch",
    # No beets album exists at the job's launch release id.
    "not_found",
    # A candidate album exists at the release id, but at least one of its
    # item paths is not under the job's launch source path (including the
    # partially-moved world where an item already reached the library
    # root). Never provably this job's own crash debris.
    "not_confined",
    # More than one candidate album is independently confined to the launch
    # source path. Cannot disambiguate which one is this job's debris.
    "ambiguous",
    # Exactly one candidate matched both preconditions and its Beets catalog
    # row was removed.
    "removed",
    # Exactly one candidate matched both preconditions, but the admitted
    # delete lane itself failed (see ``detail`` for the underlying reason).
    "removal_failed",
    # Beets itself could not be opened or queried (a known, expected
    # unavailability category — ``lib.beets_db.beets_authority_availability_category``,
    # the same classifier the world audit uses). Never a reason to remove
    # anything; recovery proceeds and the next liveness re-probe tries again.
    "beets_unavailable",
]


class RecoveryDebrisReport(msgspec.Struct, frozen=True):
    """One typed audit record of a recovery-side debris-removal attempt.

    Attached to the failed automation job's ``result`` JSONB and folded into
    its ``download_log`` detail so the outcome — removed or surfaced — is
    always Recents-visible evidence, never a silent decision.
    """

    outcome: RecoveryDebrisOutcomeKind
    album_id: int | None = None
    item_paths: tuple[str, ...] = ()
    detail: str = ""


class SupportsRecoveryDebrisBeetsDB(Protocol):
    """The read-only Beets surface this module needs to find a candidate."""

    # Declared as read-only properties, not plain attributes: real
    # ``BeetsDB.library_db_path``/``library_root`` are ``@property``, and a
    # plain mutable Protocol attribute is invariant — it would reject a
    # read-only property as "incompatible" under strict Pyright.
    @property
    def library_db_path(self) -> str: ...

    @property
    def library_root(self) -> str: ...

    def get_all_album_ids_for_release(self, release_id: str) -> list[int]: ...

    def get_album_detail(self, album_id: int) -> dict[str, object] | None: ...

    def __enter__(self) -> Self: ...

    def __exit__(self, *args: object) -> None: ...


class BeetsDbFactory(Protocol):
    def __call__(self) -> SupportsRecoveryDebrisBeetsDB: ...


class BeetsDeleteFn(Protocol):
    def __call__(self, request: BeetsDeleteRequest) -> BeetsDeleteOutcome: ...


class RecoveryDebrisRemovalFn(Protocol):
    def __call__(
        self,
        *,
        launch_release_id: str | None,
        launch_source_path: str | None,
    ) -> RecoveryDebrisReport: ...


def _item_paths_from_detail(detail: dict[str, object]) -> tuple[str, ...]:
    paths: list[str] = []
    for track in json_list(detail.get("tracks")):
        if not is_str_object_dict(track):
            continue
        path = track.get("path")
        if isinstance(path, str) and path:
            paths.append(path)
    return tuple(paths)


def _all_confined(paths: tuple[str, ...], root: Path) -> bool:
    """Require every path present AND lexically/symlink-confined to ``root``.

    Reuses ``lib.beets_delete``'s own escape check so the read-only
    inspection here can never disagree with the admitted delete lane's own,
    authoritative re-check of the same confinement.
    """
    if not paths:
        return False
    return all(_confined_path(path, root) is not None for path in paths)


def _find_debris_candidate(
    beets_db: SupportsRecoveryDebrisBeetsDB,
    *,
    launch_release_id: str,
    root: Path,
) -> RecoveryDebrisReport | tuple[int, tuple[str, ...]]:
    """Read-only search; returns a terminal report or one clean candidate."""
    candidate_ids = beets_db.get_all_album_ids_for_release(launch_release_id)
    if not candidate_ids:
        return RecoveryDebrisReport(outcome="not_found")

    confined: list[tuple[int, tuple[str, ...]]] = []
    inspected_paths: list[str] = []
    for album_id in candidate_ids:
        detail = beets_db.get_album_detail(album_id)
        if detail is None:
            continue
        paths = _item_paths_from_detail(detail)
        inspected_paths.extend(paths)
        if _all_confined(paths, root):
            confined.append((album_id, paths))

    if not confined:
        return RecoveryDebrisReport(
            outcome="not_confined",
            item_paths=tuple(inspected_paths),
        )
    if len(confined) > 1:
        return RecoveryDebrisReport(
            outcome="ambiguous",
            item_paths=tuple(
                path for _album_id, paths in confined for path in paths
            ),
        )
    return confined[0]


def remove_recovery_debris(
    *,
    launch_release_id: str | None,
    launch_source_path: str | None,
    beets_db_factory: BeetsDbFactory = BeetsDB,
    beets_delete_fn: BeetsDeleteFn = run_beets_delete,
) -> RecoveryDebrisReport:
    """Find and remove — via the admitted lane only — one job's own debris.

    Never removes anything when either precondition is unproven: a missing
    release, an ambiguous match, or any item path escaping the launch source
    (including the partially-moved world where a path already reached the
    library root). Never writes a ``source_denylist`` row — this function
    never touches the pipeline database at all.

    A known, expected Beets-unavailability failure while opening or reading
    the catalog (``lib.beets_db.beets_authority_availability_category`` —
    the same classifier ``lib.world_audit_service`` uses) is reported, never
    raised: recovery must keep moving (CLAUDE.md invariant 11) even when
    Beets itself happens to be briefly unreachable. Any OTHER exception is a
    genuine unclassified failure and propagates, exactly like every other
    step of this recovery.
    """
    if not launch_release_id or not launch_source_path:
        return RecoveryDebrisReport(outcome="no_launch")

    # ``strict=False``: the launch source folder may already be gone (a
    # resumed cleanup journal, or a prior interrupted recovery attempt) —
    # this only compares recorded item-path strings, it never touches the
    # filesystem.
    root = Path(launch_source_path).resolve(strict=False)

    try:
        with beets_db_factory() as beets_db:
            found = _find_debris_candidate(
                beets_db,
                launch_release_id=launch_release_id,
                root=root,
            )
            if isinstance(found, RecoveryDebrisReport):
                return found
            album_id, paths = found
            request = BeetsDeleteRequest(
                album_id=album_id,
                expected_release_id=launch_release_id,
                library_db_path=beets_db.library_db_path,
                library_root=beets_db.library_root,
                debris_confinement_root=str(root),
            )
    except Exception as exc:
        category = beets_authority_availability_category(exc)
        if category is None:
            raise
        return RecoveryDebrisReport(
            outcome="beets_unavailable",
            detail=f"{type(exc).__name__}: {exc}",
        )

    outcome = beets_delete_fn(request)
    if isinstance(outcome, BeetsDeleteCompleted):
        return RecoveryDebrisReport(
            outcome="removed",
            album_id=album_id,
            item_paths=paths,
        )
    return RecoveryDebrisReport(
        outcome="removal_failed",
        album_id=album_id,
        item_paths=paths,
        detail=f"{outcome.reason}: {outcome.detail}",
    )


__all__ = [
    "BeetsDbFactory",
    "BeetsDeleteFn",
    "RecoveryDebrisOutcomeKind",
    "RecoveryDebrisRemovalFn",
    "RecoveryDebrisReport",
    "SupportsRecoveryDebrisBeetsDB",
    "remove_recovery_debris",
]
