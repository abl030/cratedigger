"""Operator merge-rekey action — heal a request→Beets join after MusicBrainz
merges the release a request points at (#1089).

MusicBrainz editors merge two release entries; the loser's MBID becomes a
permanent redirect to the survivor. #1059/#1080 taught the automation and
force import lanes to follow that redirect at import time — but the
population this service acts on has ALREADY imported, under the merged-away
id, and Beets has since been retagged onto the survivor out of band (an
operator's own ``beet modify``, or a later re-import that landed there). The
pipeline ledger is the only thing still naming the old id, so the request
→Beets join silently breaks: the dashboard's disk-coverage drift panel
(``lib/disk_coverage_service.py``) reports the row as off-disk forever,
because ``BeetsDB.check_mbids`` can never again match the stored id.

**Request-ledger-only.** This service NEVER mutates Beets — that is the
settled operator correction on this slice. For every row it can legitimately
act on, Beets already holds the survivor; that is exactly what makes the row
drift. The one write is ``PipelineDB.update_request_release_for_merge``'s
operator claim arm, which moves ``album_requests.mb_release_id`` (and the
row's ``album_quality_evidence`` lineage) onto the identity Beets already
has. The three-mutation-lane boundary (serial importer harness, exact-album
delete child, import-time merge retag) is untouched.

Authority: "really we need to re-key mbid and beets don't we so they go
away. we could surface these here and have a button which re-keys with the
current machinery we've built couldn't we?" — followed by, on the
request-ledger-only refinement: "this is what I want for sure." (operator,
2026-08-13 session) —
https://github.com/abl030/cratedigger/issues/1089#issuecomment-5274933957

``pipeline-cli merge-rekey`` and ``POST /api/pipeline/<id>/merge-rekey`` are
thin adapters that wrap ``MergeRekeyService.rekey_request`` — the CLI relays
the canonical web route's response (CD-QUAL-01 shape), it does not construct
this service directly.

Outcome → exit code / HTTP status convention (matches
``lib/mbid_replace_service.py`` / ``lib/search_plan_service.py``):

    rekeyed                  200 / 0
    not_found                404 / 2
    wrong_state               409 / 4
    not_merged                422 / 3
    library_not_at_survivor   409 / 4
    rekey_refused              409 / 4
    mirror_unavailable         503 / 5
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import TYPE_CHECKING, Protocol, runtime_checkable

import msgspec

from lib.beets_db import (
    CurrentBeetsAmbiguous,
    CurrentBeetsResolution,
    CurrentBeetsUnique,
)
from lib.mb_canonical import CanonicalReleaseFn, production_canonical_release_fn
from lib.release_identity import ReleaseIdentity

if TYPE_CHECKING:
    from lib.pipeline_db.rows import AlbumRequestRow

logger = logging.getLogger(__name__)

RESULT_REKEYED = "rekeyed"
RESULT_NOT_FOUND = "not_found"
RESULT_WRONG_STATE = "wrong_state"
RESULT_MIRROR_UNAVAILABLE = "mirror_unavailable"
RESULT_NOT_MERGED = "not_merged"
RESULT_LIBRARY_NOT_AT_SURVIVOR = "library_not_at_survivor"
RESULT_REKEY_REFUSED = "rekey_refused"

#: The route's status mapping. The CLI needs no paired exit-code dict of its
#: own: every status here already matches ``_exit_code``'s default
#: status->exit mapping (200/0, 404/2, 422/3, 409/4, 503/5) in
#: ``scripts/pipeline_cli/api_mutations.py``, so ``cmd_merge_rekey`` relays
#: with no ``exit_overrides`` — see its docstring.
MERGE_REKEY_HTTP_STATUS: dict[str, int] = {
    RESULT_REKEYED: 200,
    RESULT_NOT_FOUND: 404,
    RESULT_WRONG_STATE: 409,
    RESULT_NOT_MERGED: 422,
    RESULT_LIBRARY_NOT_AT_SURVIVOR: 409,
    RESULT_REKEY_REFUSED: 409,
    RESULT_MIRROR_UNAVAILABLE: 503,
}


class MergeRekeyResult(msgspec.Struct, frozen=True):
    """Outcome of one ``rekey_request`` call.

    ``outcome`` is one of the ``RESULT_*`` constants above.
    ``beets_album_ids`` carries what Beets currently resolves at the id named
    by ``beets_checked_release_id`` — populated on ``not_merged`` (what the
    stored id itself holds, the #8792 refusal) and on
    ``library_not_at_survivor`` (what the survivor holds, when it is
    anything other than exactly one album) — so the UI can explain the
    refusal instead of a bare error string.
    """

    outcome: str
    request_id: int
    old_release_id: str | None = None
    new_release_id: str | None = None
    beets_album_id: int | None = None
    beets_checked_release_id: str | None = None
    beets_album_ids: tuple[int, ...] = ()
    error_message: str | None = None


@runtime_checkable
class MergeRekeyDB(Protocol):
    """The PipelineDB surface the operator merge-rekey action uses (#1089)."""

    def get_request(self, request_id: int) -> AlbumRequestRow | None: ...

    def update_request_release_for_merge(
        self,
        request_id: int,
        *,
        old_release_id: str,
        new_release_id: str,
        expected_import_job_id: int | None,
    ) -> bool: ...


@runtime_checkable
class MergeRekeyBeetsDB(Protocol):
    """The BeetsDB surface this action uses — read-only, on purpose.

    Request-ledger-only: this service never calls a Beets mutation method.
    """

    def resolve_current_release(
        self, identity: ReleaseIdentity,
    ) -> CurrentBeetsResolution: ...


#: Whether the shared canonical-release resolver is wired to a mirror at
#: all. ``lib.mb_canonical.canonical_release_id`` is fail-open by contract
#: (module docstring): "unconfigured", "transport failure", and "asked, no
#: redirect known" are ALL collapsed into the same ``None`` return, on
#: purpose, for the automation/force import seam that never needed to tell
#: them apart. The operator route DOES need to: #8792 (Slipknot Vol. 3) is a
#: real, live, correctly-unmerged request whose stored id genuinely has no
#: redirect — that must read as ``not_merged`` (422, an operator-facing
#: fact), never as ``mirror_unavailable`` (503, "come back once this is
#: wired"). This is the ONE thing this seam checks that
#: ``canonical_release_fn``'s return value alone cannot answer.
IsMirrorConfiguredFn = Callable[[], bool]


def _default_is_mirror_configured() -> bool:
    from lib.mb_canonical import configured_canonical_base

    return configured_canonical_base() is not None


def _beets_album_ids(resolution: CurrentBeetsResolution) -> tuple[int, ...]:
    if isinstance(resolution, CurrentBeetsUnique):
        return (resolution.album_id,)
    if isinstance(resolution, CurrentBeetsAmbiguous):
        return resolution.album_ids
    return ()


class MergeRekeyService:
    """Service for the operator merge-rekey action.

    Construct one per call (or per logical caller); it is stateless beyond
    its dependencies. ``canonical_release_fn`` defaults to the process-wide
    configured resolver (``lib.mb_canonical.production_canonical_release_fn``)
    — inert until ``configure_canonical_release_lookup`` has run at process
    startup (``web/server.py::main`` / ``scripts/importer.py::main``).
    ``is_mirror_configured_fn`` defaults to checking
    ``lib.mb_canonical.configured_canonical_base()`` directly — see
    :data:`IsMirrorConfiguredFn` for why this is a second, separate seam
    rather than inferred from ``canonical_release_fn``'s return value.
    """

    def __init__(
        self,
        db: MergeRekeyDB,
        beets_db: MergeRekeyBeetsDB,
        *,
        canonical_release_fn: CanonicalReleaseFn | None = None,
        is_mirror_configured_fn: IsMirrorConfiguredFn | None = None,
    ) -> None:
        self.db = db
        self.beets_db = beets_db
        self.canonical_release_fn = (
            canonical_release_fn
            if canonical_release_fn is not None
            else production_canonical_release_fn()
        )
        self.is_mirror_configured_fn = (
            is_mirror_configured_fn
            if is_mirror_configured_fn is not None
            else _default_is_mirror_configured
        )

    def rekey_request(self, request_id: int) -> MergeRekeyResult:
        """Rekey ``request_id`` onto MusicBrainz's current survivor, or refuse.

        1. Load the request; missing → ``not_found``.
        2. Require MB-sourced (not Discogs-only), ``status == 'imported'``,
           no automation owner attached → else ``wrong_state``. This is the
           service's own precondition, checked BEFORE any network or Beets
           call — it never spends either on a row the write would refuse
           for a reason this call can already see.
        3. The resolver must be wired to a mirror at all
           (``is_mirror_configured_fn``) → else ``mirror_unavailable``. This
           is checked BEFORE asking, because the shared resolver's own
           fail-open contract cannot distinguish "unconfigured" from "asked,
           no redirect" in its return value alone (see
           :data:`IsMirrorConfiguredFn`).
        4. Resolve the stored id's current MusicBrainz survivor at click
           time. No different survivor named (asked, no redirect known —
           the #8792 refusal) → ``not_merged``: this request was never
           merged away. Reports what Beets currently holds at the stored id
           so the UI can explain.
        5. Require Beets to resolve EXACTLY ONE album at the survivor
           (enforces "the library must already be at the survivor before
           this runs", and the #1059 Beets-moves-first ordering) → else
           ``library_not_at_survivor``.
        6. Write the operator claim arm of
           ``update_request_release_for_merge`` (``expected_import_job_id
           =None``). ``False`` → ``rekey_refused`` (the request changed
           underneath this call — status, owner, identity, or an in-flight
           import job; retry re-derives). ``True`` → ``rekeyed``.
        """
        row = self.db.get_request(request_id)
        if row is None:
            return MergeRekeyResult(
                outcome=RESULT_NOT_FOUND,
                request_id=request_id,
                error_message=f"request {request_id} not found",
            )

        identity = ReleaseIdentity.from_strict_fields(
            row.get("mb_release_id"), row.get("discogs_release_id"),
        )
        status = row.get("status")
        owner = row.get("active_automation_import_job_id")
        if (
            identity is None
            or identity.source != "musicbrainz"
            or status != "imported"
            or owner is not None
        ):
            return MergeRekeyResult(
                outcome=RESULT_WRONG_STATE,
                request_id=request_id,
                old_release_id=(
                    identity.release_id if identity is not None else None
                ),
                error_message=(
                    f"request {request_id} is not an owner-free imported "
                    "MusicBrainz-sourced request (status="
                    f"{status!r}, active_automation_import_job_id={owner!r}, "
                    f"discogs_release_id={row.get('discogs_release_id')!r})"
                ),
            )
        old_release_id = identity.release_id

        if not self.is_mirror_configured_fn():
            return MergeRekeyResult(
                outcome=RESULT_MIRROR_UNAVAILABLE,
                request_id=request_id,
                old_release_id=old_release_id,
                error_message=(
                    "MusicBrainz merge-survivor resolution is not "
                    "configured on this process"
                ),
            )

        # ``canonical_release_fn``'s contract (lib/mb_canonical.py) never
        # returns the stored id itself — a cosmetic redirect that lands on
        # the same id is ALSO reported as ``None`` ("no different canonical
        # is known"). This seam re-checks rather than trusting that contract
        # (M3 in tests/test_merge_rekey.py's #1059 seam: "the resolver's own
        # contract already forbids returning the stored id... but this seam
        # authorizes a rekey; it re-checks"): a survivor equal to the stored
        # id, or one that is not itself a MusicBrainz identity, is folded
        # into the same "no legitimate redirect" answer as a bare ``None`` —
        # never passed on to the write, which would otherwise raise
        # ``ValueError`` on a self-rekey.
        raw_survivor = self.canonical_release_fn(old_release_id)
        survivor_identity = (
            ReleaseIdentity.from_id(raw_survivor) if raw_survivor else None
        )
        survivor = (
            survivor_identity.release_id
            if survivor_identity is not None
            and survivor_identity.source == "musicbrainz"
            and survivor_identity.release_id != old_release_id
            else None
        )
        if not survivor:
            stored_resolution = self.beets_db.resolve_current_release(
                ReleaseIdentity(
                    source="musicbrainz", release_id=old_release_id,
                ),
            )
            return MergeRekeyResult(
                outcome=RESULT_NOT_MERGED,
                request_id=request_id,
                old_release_id=old_release_id,
                new_release_id=old_release_id,
                beets_checked_release_id=old_release_id,
                beets_album_ids=_beets_album_ids(stored_resolution),
                error_message=(
                    f"MusicBrainz names no merge survivor for "
                    f"{old_release_id}; this request has not been merged"
                ),
            )

        survivor_resolution = self.beets_db.resolve_current_release(
            ReleaseIdentity(source="musicbrainz", release_id=survivor),
        )
        if not isinstance(survivor_resolution, CurrentBeetsUnique):
            return MergeRekeyResult(
                outcome=RESULT_LIBRARY_NOT_AT_SURVIVOR,
                request_id=request_id,
                old_release_id=old_release_id,
                new_release_id=survivor,
                beets_checked_release_id=survivor,
                beets_album_ids=_beets_album_ids(survivor_resolution),
                error_message=(
                    f"Beets does not resolve exactly one album at survivor "
                    f"{survivor}; the library must already hold the "
                    "survivor before the ledger can follow it"
                ),
            )

        applied = self.db.update_request_release_for_merge(
            request_id,
            old_release_id=old_release_id,
            new_release_id=survivor,
            expected_import_job_id=None,
        )
        if not applied:
            return MergeRekeyResult(
                outcome=RESULT_REKEY_REFUSED,
                request_id=request_id,
                old_release_id=old_release_id,
                new_release_id=survivor,
                beets_album_id=survivor_resolution.album_id,
                error_message=(
                    f"request {request_id} changed underneath the rekey "
                    "(status, owner, identity, or an in-flight import job) "
                    "— retry"
                ),
            )
        return MergeRekeyResult(
            outcome=RESULT_REKEYED,
            request_id=request_id,
            old_release_id=old_release_id,
            new_release_id=survivor,
            beets_album_id=survivor_resolution.album_id,
        )


__all__ = [
    "MERGE_REKEY_HTTP_STATUS",
    "RESULT_LIBRARY_NOT_AT_SURVIVOR",
    "RESULT_MIRROR_UNAVAILABLE",
    "RESULT_NOT_FOUND",
    "RESULT_NOT_MERGED",
    "RESULT_REKEYED",
    "RESULT_REKEY_REFUSED",
    "RESULT_WRONG_STATE",
    "IsMirrorConfiguredFn",
    "MergeRekeyBeetsDB",
    "MergeRekeyDB",
    "MergeRekeyResult",
    "MergeRekeyService",
]
