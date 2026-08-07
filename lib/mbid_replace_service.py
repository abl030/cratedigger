"""Replace operator action — supersede an album_requests row with a new
row targeting a different MusicBrainz release ID in the same release
group.

The service is the single source of truth for the Replace action;
``pipeline-cli replace`` and ``POST /api/pipeline/<id>/replace`` are
thin adapters that wrap ``MbidReplaceService.replace_request_mbid``.

Outcome → exit code / HTTP status convention (matches
``lib/search_plan_service.py``):

    replaced                       200 / 0
    not_found                      404 / 2
    wrong_state                    409 / 4
    target_invalid                 422 / 3
    target_release_group_mismatch  422 / 3
    target_same_as_current         422 / 3
    target_collision_request       409 / 4
    mirror_unconfigured            503 / 5
    transient                      503 / 5

Both MusicBrainz and Discogs sources flow through this one service; the
pathway is inferred from the id's shape (``detect_release_source``). MB×MB
is the original path, unchanged; Discogs×Discogs anchors on the source's
Discogs master (numeric id in ``mb_release_group_id``, KTD-1).

See ``docs/plans/2026-07-04-001-feat-discogs-pathway-replace-plan.md`` and
``docs/plans/2026-05-18-001-feat-replace-operator-action-plan.md`` for the
full design.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import socket
import sys
from collections.abc import Callable, Mapping
from contextlib import AbstractContextManager
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable
from urllib.error import URLError

import msgspec

if TYPE_CHECKING:
    from lib.pipeline_db._shared import ProcessingOwnerProjection
    from lib.pipeline_db.rows import AlbumRequestRow


# MB-mirror transient errors — network blips, timeouts, malformed
# JSON. These warrant RESULT_TRANSIENT (503 / exit 5; retryable),
# not RESULT_TARGET_INVALID (which signals an operator input
# violation and is not retryable).
_TRANSIENT_LOOKUP_EXCEPTIONS: tuple[type[BaseException], ...] = (
    URLError,
    TimeoutError,
    socket.timeout,
    ConnectionError,
    json.JSONDecodeError,
)

from lib import transitions
from lib.beets_db import (
    CurrentBeetsAmbiguous,
    CurrentBeetsResolution,
    CurrentBeetsUnique,
)
from lib.beets_delete import (
    BeetsDeleteFailed,
    BeetsDeleteOutcome,
    BeetsDeleteRequest,
    run_beets_delete,
)
from lib.config import CratediggerConfig
from lib.import_execution import (
    CancellationToken,
    OwnerSessionIdentity,
    OwnerSessionProbe,
)
from lib.pipeline_db import (
    ADVISORY_LOCK_NAMESPACE_IMPORT,
    MbidCollisionError,
    SupersedeRaceError,
)
from lib.pipeline_db._core import AdvisoryLockSessionLost, OwnerSessionLost
from lib.processing_paths import stage_to_ai_path
from lib.release_association_locks import release_identity_locks
from lib.release_identity import (
    ReleaseIdentity,
    detect_release_source,
    normalize_release_id,
)
from lib.replace_status import (
    REPLACE_REASON_CROSS_PATHWAY_TARGET,
    REPLACE_REASON_CURRENT_BEETS_AMBIGUOUS,
    REPLACE_REASON_CURRENT_BEETS_UNAVAILABLE,
    REPLACE_REASON_LOCK_CONTENDED,
    REPLACE_REASON_POST_SUPERSEDE_PARTIAL,
    REPLACE_REASON_SOURCE_IDENTITY_INVALID,
    REPLACE_REASON_SOURCE_NO_RELEASE_GROUP,
    REPLACE_REASON_TARGET_NO_RELEASE_GROUP,
    REPLACE_REASON_UNEXPECTED_LOOKUP_ERROR,
    REPLACE_REASON_UNRESOLVABLE_TARGET,
    RESULT_MIRROR_UNCONFIGURED,
    RESULT_NOT_FOUND,
    RESULT_REPLACED,
    RESULT_TARGET_COLLISION_REQUEST,
    RESULT_TARGET_INVALID,
    RESULT_TARGET_RELEASE_GROUP_MISMATCH,
    RESULT_TARGET_SAME_AS_CURRENT,
    RESULT_TRANSIENT,
    RESULT_WRONG_STATE,
)
from lib.request_identity import acceptable_identities, resolve_current_for_request
from lib.search_plan_service import SearchPlanDB, SearchPlanService
from lib.util import (
    trigger_jellyfin_scan,
    trigger_plex_scan,
)
from lib.wrong_match_delete_service import (
    WrongMatchDeleteDB,
    WrongMatchDeleteSummary,
    delete_wrong_match_group,
)

type TargetData = dict[str, Any]

logger = logging.getLogger(__name__)


@runtime_checkable
class MbidReplaceDB(
    WrongMatchDeleteDB, SearchPlanDB, Protocol,
):
    """The PipelineDB surface the Replace action uses (#409).

    Extends the protocols of everything the handle is forwarded into:
    ``delete_wrong_match_group`` and the constructor-built
    ``SearchPlanService``. Parity tests live in
    ``tests/test_mbid_replace_service.py``.
    """

    def get_request_by_mb_release_id(
        self, mb_release_id: str,
    ) -> AlbumRequestRow | None: ...

    def get_request_by_release_id(
        self, release_id: object | None,
    ) -> AlbumRequestRow | None: ...

    def get_request_by_replaces_request_id(
        self, replaced_id: int,
    ) -> AlbumRequestRow | None: ...

    def _pin_owner_session(
        self, token: CancellationToken,
    ) -> AbstractContextManager[OwnerSessionIdentity]: ...

    def _probe_owner_session(
        self,
        identity: OwnerSessionIdentity,
        *,
        deadline_seconds: float = 0.75,
    ) -> OwnerSessionProbe: ...

    def supersede_request_mbid(
        self,
        old_request_id: int,
        *,
        new_mb_release_id: str,
        new_mb_release_group_id: str | None,
        new_mb_artist_id: str | None,
        new_artist_name: str,
        new_album_title: str,
        new_year: int | None,
        new_country: str | None,
        new_tracks: list[dict[str, Any]],
        new_discogs_release_id: str | None = None,
    ) -> int: ...


class ReplaceResult(msgspec.Struct, frozen=True):
    """Outcome of a single ``replace_request_mbid`` call.

    ``outcome`` is one of the ``RESULT_*`` constants (imported from
    ``lib.replace_status`` and re-exported here — CLI/API/tests import
    them from this module). Other fields are surfaced conditionally:

    - ``new_request_id``: set on ``RESULT_REPLACED``.
    - ``current_status``: set on ``RESULT_TARGET_COLLISION_REQUEST`` so
      the UI can render "already in pipeline (status=imported)" or the
      "previously abandoned" warning when the existing row is
      ``replaced``.
    - ``descendant_request_id``: set on ``RESULT_WRONG_STATE`` when the
      source row is itself already ``status='replaced'`` — so the UI
      can deep-link to "the new request is at /pipeline/{id}".
    - ``reason``: a ``REPLACE_REASON_*`` constant on typed rejection
      outcomes, distinguishing failures that an outcome alone collapses.
      ``error_message`` stays free-text for operator-facing detail;
      ``reason`` is the stable code CLI/API/tests assert on.
      ``msgspec.Struct`` per the wire-boundary rule (CLI ``--json`` output
      and the HTTP response body both surface every field).
    - ``warnings``: filesystem-cleanup failures that did NOT roll back
      the DB change (R26 non-fatal semantics).
    """

    outcome: str
    request_id: int
    new_request_id: int | None = None
    current_status: str | None = None
    descendant_request_id: int | None = None
    error_message: str | None = None
    reason: str | None = None
    warnings: tuple[str, ...] = ()
    processing_owner: ProcessingOwnerProjection | None = None


def _processing_locked_replace(
    row: Mapping[str, object],
    request_id: int,
) -> ReplaceResult | None:
    conflict = transitions.processing_locked_conflict(
        row,
        request_id,
        "replaced",
        expected_status=str(row["status"]),
    )
    if conflict is None:
        return None
    owner = conflict.processing_owner
    if owner is None:
        raise RuntimeError(
            "processing conflict is missing its exact owner"
        )
    return ReplaceResult(
        outcome=RESULT_WRONG_STATE,
        request_id=request_id,
        reason=transitions.TransitionConflictKind.processing_locked.value,
        error_message=(
            f"request {request_id} is owned by automation import job "
            f"{owner.job_id}"
        ),
        processing_owner=owner,
    )


# Type aliases for the injectable dependencies.
MBLookup = Callable[..., dict[str, Any]]
"""Signature: ``mb_lookup(mbid, *, fresh: bool=False) -> dict``. The
default is ``web.mb.get_release``; tests inject a fake."""

DiscogsLookup = Callable[..., dict[str, Any]]
"""Signature: ``discogs_lookup(release_id: int, *, fresh: bool=False) ->
dict``. The default is ``web.discogs.get_release``; tests inject a fake
that raises the real ``HTTPError``/``URLError``/``DiscogsMirrorNotConfigured``
on failure paths (test-fidelity Rule B)."""

class ReplaceBeetsDB(Protocol):
    @property
    def library_db_path(self) -> str: ...

    @property
    def library_root(self) -> str: ...

    def resolve_current_release(
        self, identity: ReleaseIdentity,
    ) -> CurrentBeetsResolution: ...

    def resolve_current_releases(
        self, identities: list[ReleaseIdentity],
    ) -> dict[ReleaseIdentity, CurrentBeetsResolution]: ...

    def close(self) -> None: ...


BeetsDBFactory = Callable[[], ReplaceBeetsDB]
"""Zero-arg callable returning a ``BeetsDB`` instance. Default uses
``lib.beets_db.BeetsDB`` against the configured library path."""

BeetsDeleteFn = Callable[[BeetsDeleteRequest], BeetsDeleteOutcome]
"""Injectable pinned exact-album deletion boundary."""

WrongMatchDeleteFn = Callable[[MbidReplaceDB, int], WrongMatchDeleteSummary]
"""Injectable post-replace wrong-match cleanup boundary."""


def _default_mb_lookup(mbid: str, *, fresh: bool = False) -> dict[str, Any]:
    """Default MB-mirror lookup. Imported lazily so the service module
    doesn't pull in ``web.mb``'s urllib transport at import time."""
    from web.mb import get_release
    return get_release(mbid, fresh=fresh)


def _default_discogs_lookup(
    release_id: int, *, fresh: bool = False,
) -> dict[str, Any]:
    """Default Discogs-mirror lookup. Imported lazily so the service
    module doesn't pull in ``web.discogs``'s transport at import time."""
    from web.discogs import get_release
    return get_release(release_id, fresh=fresh)


def _default_beets_db_factory() -> ReplaceBeetsDB:
    """Default beets DB factory — production callers pass an explicit
    factory but tests and CLI scripts use this fallback."""
    from lib.beets_db import open_beets_db
    return open_beets_db()


class MbidReplaceService:
    """Service for the Replace operator action.

    Construct one per process (or per logical caller). The service is
    stateless beyond its dependencies.
    """

    def __init__(
        self,
        db: MbidReplaceDB,
        config: CratediggerConfig,
        slskd: Any = None,
        beets_db_factory: BeetsDBFactory | None = None,
        mb_lookup: MBLookup | None = None,
        discogs_lookup: DiscogsLookup | None = None,
        search_plan_service: SearchPlanService | None = None,
        beets_delete_fn: BeetsDeleteFn | None = None,
        wrong_match_delete_fn: WrongMatchDeleteFn | None = None,
    ) -> None:
        self.db = db
        self.config = config
        # slskd is accepted for API symmetry with the rest of the
        # pipeline services but Replace intentionally never touches
        # in-flight transfers (R23 — orphans deferred to issue #278).
        self.slskd = slskd
        self.beets_db_factory = beets_db_factory or _default_beets_db_factory
        self.mb_lookup = mb_lookup or _default_mb_lookup
        self.discogs_lookup = discogs_lookup or _default_discogs_lookup
        self.search_plan_service = (
            search_plan_service or SearchPlanService(db, config)
        )
        self.beets_delete_fn = (
            beets_delete_fn if beets_delete_fn is not None else run_beets_delete
        )
        self.wrong_match_delete_fn = (
            wrong_match_delete_fn
            if wrong_match_delete_fn is not None
            else delete_wrong_match_group
        )

    def replace_request_mbid(
        self,
        request_id: int,
        *,
        target_mb_release_id: str,
    ) -> ReplaceResult:
        try:
            return self._replace_request_mbid(
                request_id, target_mb_release_id=target_mb_release_id,
            )
        except AdvisoryLockSessionLost:
            # The lock-bearing backend died.  A supersede may already have
            # committed, in which case retrying would be wrong: expose the
            # runnable descendant and let ordinary plan/cycle convergence
            # finish its derived work.  Before supersede this is a normal
            # retryable busy result with no mutation.
            try:
                descendant = self.db.get_request_by_replaces_request_id(
                    request_id,
                )
            except Exception:  # noqa: BLE001 - session loss is primary
                descendant = None
            if descendant is not None:
                return ReplaceResult(
                    outcome=RESULT_TRANSIENT,
                    request_id=request_id,
                    descendant_request_id=int(descendant["id"]),
                    reason=REPLACE_REASON_POST_SUPERSEDE_PARTIAL,
                    error_message=(
                        "Replace session was lost after supersede; the new "
                        f"request {descendant['id']} needs tail resumption"
                    ),
                )
            return ReplaceResult(
                outcome=RESULT_TRANSIENT,
                request_id=request_id,
                reason=REPLACE_REASON_LOCK_CONTENDED,
                error_message="Replace lock session was lost; retry",
            )

    def _replace_request_mbid(
        self,
        request_id: int,
        *,
        target_mb_release_id: str,
    ) -> ReplaceResult:
        """Supersede ``request_id`` with a new row at ``target_mb_release_id``.

        Phases:

        0. Validate (read-only): load source row, double-click early
           exit, target-same-as-current, lazy-backfill source RG,
           pre-check target collision, fresh MB lookup, RG match,
           canonical-redirect re-check.
        1. Acquire the per-request IMPORT advisory lock; refuse on
           contention (no pre-emption — the importer worker holds it).
        2. Re-read the source row under the lock and capture
           pre-supersede state (artist/title for staging path, exact release
           identity, status), then resolve the current Beets album snapshot.
        3. DB transaction: ``supersede_request_mbid`` atomically flips
           the old row's status, inserts the new row, and inserts tracks.
        4. Filesystem cleanup (non-fatal warnings collected):
           - beets removal of the old release whenever its id resolves —
             request status is irrelevant (backfill rows are wanted with
             an install on disk; ``clear_pipeline_state=False`` so
             characteristic fields stay frozen on the audit row)
           - wrong-matches group delete
           - staging folder rmtree (skipped when old was downloading)
        5. Post-cleanup (advisory lock RELEASED first): regenerate
           search plan for the new request, trigger Plex /
           Jellyfin rescans. The lock is dropped before these run
           because rescans each carry their own ~10s timeout and the
           new request has ``active_plan_id=NULL`` until SearchPlanService
           runs, so no importer worker would contend for it anyway.
        """
        logger.info(
            "Replace: request_id=%d target_mb_release_id=%s",
            request_id, target_mb_release_id,
        )
        # Phase 0 — validate.
        source = self.db.get_request(request_id)
        if source is None:
            return ReplaceResult(
                outcome=RESULT_NOT_FOUND,
                request_id=request_id,
                error_message=f"request {request_id} not found",
            )
        processing_locked = _processing_locked_replace(source, request_id)
        if processing_locked is not None:
            return processing_locked

        # Step 1a — double-click / already-replaced source. The frozen
        # audit row cannot mint another descendant. The same exact requested
        # target is instead the idempotent post-supersede tail-resume key.
        if source.get("status") == "replaced":
            descendant = self.db.get_request_by_replaces_request_id(
                request_id
            )
            target_identity = ReleaseIdentity.from_id(target_mb_release_id)
            descendant_identity = (
                ReleaseIdentity.from_strict_fields(
                    descendant.get("mb_release_id"),
                    descendant.get("discogs_release_id"),
                ) if descendant is not None else None
            )
            if (
                descendant is not None
                and target_identity is not None
                and target_identity == descendant_identity
            ):
                return self._resume_replaced_tail(
                    request_id,
                    target_identity=target_identity,
                )
            return ReplaceResult(
                outcome=RESULT_WRONG_STATE,
                request_id=request_id,
                descendant_request_id=(
                    int(descendant["id"]) if descendant else None
                ),
                error_message=(
                    f"request {request_id} has already been replaced"
                ),
            )

        source_identity = ReleaseIdentity.from_strict_fields(
            source.get("mb_release_id"),
            source.get("discogs_release_id"),
        )
        if source_identity is None:
            return ReplaceResult(
                outcome=RESULT_WRONG_STATE,
                request_id=request_id,
                reason=REPLACE_REASON_SOURCE_IDENTITY_INVALID,
                error_message=(
                    f"request {request_id} has missing, malformed, or "
                    "conflicting exact release identity fields"
                ),
            )
        source_mbid = source_identity.release_id

        # Pathway-aware target gate (replaces the old step-0a UUID gate).
        # The target must be a valid release id in the SAME identity space
        # as the source: UUID target ⇒ MB source, numeric target ⇒ Discogs
        # source. A cross-pathway target (or an unparseable id) is
        # RESULT_TARGET_INVALID — cross-pathway Replace is out of scope
        # (R4 / AE2). ``detect_release_source`` is the single authority for
        # the pathway (KTD-2); the branch below dispatches on the source's
        # own shape, so MB×MB flows through the original path untouched.
        source_source = source_identity.source
        target_source = detect_release_source(target_mb_release_id)
        if (
            target_source not in ("musicbrainz", "discogs")
            or target_source != source_source
        ):
            return ReplaceResult(
                outcome=RESULT_TARGET_INVALID,
                request_id=request_id,
                error_message=(
                    f"target {target_mb_release_id!r} ({target_source}) is "
                    f"not a valid same-pathway target for source "
                    f"({source_source})"
                ),
                reason=REPLACE_REASON_CROSS_PATHWAY_TARGET,
            )

        if source_mbid == target_mb_release_id:
            return ReplaceResult(
                outcome=RESULT_TARGET_SAME_AS_CURRENT,
                request_id=request_id,
                error_message=(
                    "target MBID equals the source request's current "
                    "MBID"
                ),
            )

        if source_source == "discogs":
            return self._replace_discogs_target(
                request_id, source, source_mbid, target_mb_release_id,
            )

        source_rg = source.get("mb_release_group_id")
        if not source_rg:
            # Lazy-backfill: resolve the source MBID's RG fresh.
            src_data, err = self._mb_lookup_or_error(
                source_mbid,
                request_id=request_id,
                detail_context=f"source MBID {source_mbid}",
            )
            if err is not None:
                return err
            assert src_data is not None
            # ``mb_lookup`` is typed dict[str, Any]; ``release_group_id``
            # is None when the mirror doesn't have one.
            source_rg = src_data.get("release_group_id")
            if not source_rg:
                return ReplaceResult(
                    outcome=RESULT_TARGET_INVALID,
                    request_id=request_id,
                    error_message=(
                        f"source MBID {source_mbid} did not resolve to "
                        "a release group on the MB mirror"
                    ),
                    reason=REPLACE_REASON_SOURCE_NO_RELEASE_GROUP,
                )

        # Pre-check collision against the active row set.
        existing = self.db.get_request_by_mb_release_id(target_mb_release_id)
        if existing is not None and int(existing["id"]) != request_id:
            return ReplaceResult(
                outcome=RESULT_TARGET_COLLISION_REQUEST,
                request_id=request_id,
                current_status=existing.get("status"),
                error_message=(
                    f"target MBID {target_mb_release_id} is already used "
                    f"by request {existing['id']} "
                    f"(status={existing.get('status')!r})"
                ),
            )

        # Fresh MB lookup of the target.
        target_data, err = self._mb_lookup_or_error(
            target_mb_release_id,
            request_id=request_id,
            detail_context=f"target MBID {target_mb_release_id}",
        )
        if err is not None:
            return err
        assert target_data is not None

        if not target_data:
            return ReplaceResult(
                outcome=RESULT_TARGET_INVALID,
                request_id=request_id,
                error_message=(
                    f"target MBID {target_mb_release_id} returned empty "
                    "payload from MB mirror"
                ),
                reason=REPLACE_REASON_UNRESOLVABLE_TARGET,
            )

        canonical_mbid = target_data.get("id") or target_mb_release_id
        target_rg = target_data.get("release_group_id")
        if not target_rg:
            return ReplaceResult(
                outcome=RESULT_TARGET_INVALID,
                request_id=request_id,
                error_message=(
                    f"target MBID {target_mb_release_id} resolved with "
                    "no release_group_id"
                ),
                reason=REPLACE_REASON_TARGET_NO_RELEASE_GROUP,
            )

        if target_rg != source_rg:
            return ReplaceResult(
                outcome=RESULT_TARGET_RELEASE_GROUP_MISMATCH,
                request_id=request_id,
                error_message=(
                    f"target release group {target_rg} does not match "
                    f"source release group {source_rg}"
                ),
            )

        # Handle MB 301 redirect: if the canonical MBID differs from
        # what the operator requested, re-check collision against the
        # canonical and (defensively) against the source.
        if canonical_mbid != target_mb_release_id:
            if canonical_mbid == source_mbid:
                return ReplaceResult(
                    outcome=RESULT_TARGET_COLLISION_REQUEST,
                    request_id=request_id,
                    current_status=source.get("status"),
                    error_message=(
                        f"target MBID {target_mb_release_id} redirects "
                        f"to canonical {canonical_mbid} which is the "
                        "source's current MBID"
                    ),
                )
            existing_canon = self.db.get_request_by_mb_release_id(
                canonical_mbid
            )
            if (
                existing_canon is not None
                and int(existing_canon["id"]) != request_id
            ):
                return ReplaceResult(
                    outcome=RESULT_TARGET_COLLISION_REQUEST,
                    request_id=request_id,
                    current_status=existing_canon.get("status"),
                    error_message=(
                        f"target redirects to canonical "
                        f"{canonical_mbid} held by request "
                        f"{existing_canon['id']} "
                        f"(status={existing_canon.get('status')!r})"
                    ),
                )

        return self._finalize_replace(
            request_id,
            canonical_mbid=canonical_mbid,
            target_rg=target_rg,
            target_data=target_data,
            new_discogs_release_id=None,
        )

    def _mb_lookup_or_error(
        self,
        mbid: str,
        *,
        request_id: int,
        detail_context: str,
    ) -> tuple[dict[str, Any] | None, ReplaceResult | None]:
        """Fresh MB-mirror lookup + the two-way exception→outcome mapping
        shared by the source lazy-backfill and target lookup sites in
        ``replace_request_mbid``. Mirrors ``_discogs_lookup_or_error``
        (#501 item 3) — no ``mirror_unconfigured`` branch, since the MB
        mirror has no analogous "unconfigured" failure mode (public MB is
        the always-available fallback).

        Returns ``(data, None)`` on success or ``(None, ReplaceResult(...))``
        on failure. ``detail_context`` names the id being resolved in the
        generic RESULT_TARGET_INVALID message (e.g. ``"source MBID
        <id>"`` / ``"target MBID <id>"``), preserving each call site's
        original wording.

        A network blip / timeout / malformed JSON is RESULT_TRANSIENT
        (503, retryable); anything else is RESULT_TARGET_INVALID (422)
        AND logs a warning, so a real bug in the mirror client no longer
        presents identically to bad operator input.
        """
        try:
            data = self.mb_lookup(mbid, fresh=True)
        except _TRANSIENT_LOOKUP_EXCEPTIONS as exc:
            return None, ReplaceResult(
                outcome=RESULT_TRANSIENT,
                request_id=request_id,
                error_message=f"MB lookup failed (transient): {exc}",
            )
        except Exception as exc:  # noqa: BLE001 - boundary converts or isolates collaborator failures
            logger.warning(
                "Replace: unexpected MB lookup error resolving %s "
                "(request_id=%d): %s: %s",
                detail_context, request_id, type(exc).__name__, exc,
            )
            return None, ReplaceResult(
                outcome=RESULT_TARGET_INVALID,
                request_id=request_id,
                error_message=(
                    f"{detail_context} could not be resolved: {exc}"
                ),
                reason=REPLACE_REASON_UNEXPECTED_LOOKUP_ERROR,
            )
        return data, None

    def _replace_discogs_target(
        self,
        request_id: int,
        source: Mapping[str, Any],
        source_mbid: str | None,
        target_mb_release_id: str,
    ) -> ReplaceResult:
        """Discogs arm of Phase 0 — mirror of the MB decision order
        (guardrails before IO), then delegate to the shared Phase 1-5.

        Reached only when both the source and target are Discogs-pathway
        (numeric) ids and the target differs from the source. The source's
        Discogs master lives in ``mb_release_group_id`` (numeric, KTD-1);
        legacy rows with a NULL master lazy-resolve it via a fresh lookup
        of the source id (no persist needed — the old row is about to
        freeze, and the superseded-into row carries the master directly).
        Collision checks go through the identity-aware
        ``get_request_by_release_id`` (KTD-6); the MB arm's call sites stay
        on ``get_request_by_mb_release_id``.
        """
        normalized_target = normalize_release_id(target_mb_release_id)
        target_id_num = int(normalized_target)

        # Resolve the source master (guardrail before the target IO).
        source_master = source.get("mb_release_group_id")
        if not source_master:
            src_data, err = self._discogs_lookup_or_error(
                int(normalize_release_id(source_mbid)),
                request_id=request_id,
                detail_context=f"source Discogs id {source_mbid}",
            )
            if err is not None:
                return err
            assert src_data is not None
            source_master = src_data.get("release_group_id")
            if not source_master:
                # Masterless source: the only valid target is the source
                # itself, already caught by RESULT_TARGET_SAME_AS_CURRENT
                # upstream. Any other target crosses albums (AE1 / R10).
                return ReplaceResult(
                    outcome=RESULT_TARGET_INVALID,
                    request_id=request_id,
                    error_message=(
                        f"source Discogs release {source_mbid} has no "
                        "master; nothing to swap to (only the current "
                        "release is a valid target)"
                    ),
                    reason=REPLACE_REASON_SOURCE_NO_RELEASE_GROUP,
                )

        # Pre-check collision against the raw target id (identity-aware).
        existing = self.db.get_request_by_release_id(target_mb_release_id)
        if existing is not None and int(existing["id"]) != request_id:
            return ReplaceResult(
                outcome=RESULT_TARGET_COLLISION_REQUEST,
                request_id=request_id,
                current_status=existing.get("status"),
                error_message=(
                    f"target Discogs id {target_mb_release_id} is already "
                    f"used by request {existing['id']} "
                    f"(status={existing.get('status')!r})"
                ),
            )

        # Fresh Discogs lookup of the target.
        target_data, err = self._discogs_lookup_or_error(
            target_id_num,
            request_id=request_id,
            detail_context=f"target Discogs id {target_mb_release_id}",
        )
        if err is not None:
            return err
        assert target_data is not None

        if not target_data:
            return ReplaceResult(
                outcome=RESULT_TARGET_INVALID,
                request_id=request_id,
                error_message=(
                    f"target Discogs id {target_mb_release_id} returned "
                    "empty payload from the mirror"
                ),
                reason=REPLACE_REASON_UNRESOLVABLE_TARGET,
            )

        canonical_id = str(target_data.get("id") or target_mb_release_id)
        target_master = target_data.get("release_group_id")
        if not target_master:
            return ReplaceResult(
                outcome=RESULT_TARGET_INVALID,
                request_id=request_id,
                error_message=(
                    f"target Discogs id {target_mb_release_id} resolved "
                    "with no master"
                ),
                reason=REPLACE_REASON_TARGET_NO_RELEASE_GROUP,
            )

        if target_master != source_master:
            return ReplaceResult(
                outcome=RESULT_TARGET_RELEASE_GROUP_MISMATCH,
                request_id=request_id,
                error_message=(
                    f"target master {target_master} does not match source "
                    f"master {source_master}"
                ),
            )

        # Canonical-redirect re-check (mirror the MB arm): if the mirror
        # returned a different canonical id, re-check collision against it
        # and (defensively) against the source.
        if canonical_id != normalized_target:
            if canonical_id == normalize_release_id(source_mbid):
                return ReplaceResult(
                    outcome=RESULT_TARGET_COLLISION_REQUEST,
                    request_id=request_id,
                    current_status=source.get("status"),
                    error_message=(
                        f"target Discogs id {target_mb_release_id} "
                        f"redirects to canonical {canonical_id} which is "
                        "the source's current id"
                    ),
                )
            existing_canon = self.db.get_request_by_release_id(canonical_id)
            if (
                existing_canon is not None
                and int(existing_canon["id"]) != request_id
            ):
                return ReplaceResult(
                    outcome=RESULT_TARGET_COLLISION_REQUEST,
                    request_id=request_id,
                    current_status=existing_canon.get("status"),
                    error_message=(
                        f"target redirects to canonical {canonical_id} "
                        f"held by request {existing_canon['id']} "
                        f"(status={existing_canon.get('status')!r})"
                    ),
                )

        return self._finalize_replace(
            request_id,
            canonical_mbid=canonical_id,
            target_rg=target_master,
            target_data=target_data,
            new_discogs_release_id=canonical_id,
        )

    def _discogs_lookup_or_error(
        self,
        release_id_num: int,
        *,
        request_id: int,
        detail_context: str,
    ) -> tuple[dict[str, Any] | None, ReplaceResult | None]:
        """Fresh Discogs-mirror lookup + the three-way exception→outcome
        mapping shared by the source lazy-backfill and target lookup sites
        in ``_replace_discogs_target``.

        Returns ``(data, None)`` on success or ``(None, ReplaceResult(...))``
        on failure. ``detail_context`` names the id being resolved in the
        generic RESULT_TARGET_INVALID message (e.g. ``"source Discogs id
        1001"`` / ``"target Discogs id 1002"``), preserving each call
        site's original wording.

        The three failure classes mirror the MB arm: an unconfigured
        mirror is RESULT_MIRROR_UNCONFIGURED (503), a network blip /
        timeout / malformed JSON is RESULT_TRANSIENT (503, retryable),
        and anything else is RESULT_TARGET_INVALID (422). The generic
        branch ALSO logs a warning so a real bug in the mirror client no
        longer presents identically to bad operator input.
        """
        from web.discogs import DiscogsMirrorNotConfigured

        try:
            data = self.discogs_lookup(release_id_num, fresh=True)
        except DiscogsMirrorNotConfigured as exc:
            return None, ReplaceResult(
                outcome=RESULT_MIRROR_UNCONFIGURED,
                request_id=request_id,
                error_message=f"Discogs mirror not configured: {exc}",
            )
        except _TRANSIENT_LOOKUP_EXCEPTIONS as exc:
            return None, ReplaceResult(
                outcome=RESULT_TRANSIENT,
                request_id=request_id,
                error_message=f"Discogs lookup failed (transient): {exc}",
            )
        except Exception as exc:  # noqa: BLE001 - boundary converts or isolates collaborator failures
            logger.warning(
                "Replace: unexpected Discogs lookup error resolving %s "
                "(request_id=%d): %s: %s",
                detail_context, request_id, type(exc).__name__, exc,
            )
            return None, ReplaceResult(
                outcome=RESULT_TARGET_INVALID,
                request_id=request_id,
                error_message=(
                    f"{detail_context} could not be resolved: {exc}"
                ),
                reason=REPLACE_REASON_UNEXPECTED_LOOKUP_ERROR,
            )
        return data, None

    def _resume_replaced_tail(
        self,
        request_id: int,
        *,
        target_identity: ReleaseIdentity,
    ) -> ReplaceResult:
        """Converge a committed Replace without inventing another request.

        ``replaces_request_id`` is the durable receipt for the only allowed
        descendant.  The request target is checked before locking and again
        under IMPORT plus the complete old/new RELEASE-key union. Each tail
        effect is monotonic: an interrupted deletion or directory cleanup is
        simply re-derived and retried by the same exact Replace invocation.
        """
        token = CancellationToken()
        warnings: list[str] = []
        try:
            with self.db._pin_owner_session(token) as owner:
                with self.db.advisory_lock(
                    ADVISORY_LOCK_NAMESPACE_IMPORT, request_id,
                ) as acquired:
                    if not acquired:
                        return ReplaceResult(
                            outcome=RESULT_TRANSIENT,
                            request_id=request_id,
                            reason=REPLACE_REASON_LOCK_CONTENDED,
                            error_message="Replace source is busy; retry tail resumption",
                        )
                    source = self.db.get_request(request_id)
                    descendant = self.db.get_request_by_replaces_request_id(
                        request_id,
                    )
                    source_identity = (
                        ReleaseIdentity.from_strict_fields(
                            source.get("mb_release_id"),
                            source.get("discogs_release_id"),
                        ) if source is not None else None
                    )
                    descendant_identity = (
                        ReleaseIdentity.from_strict_fields(
                            descendant.get("mb_release_id"),
                            descendant.get("discogs_release_id"),
                        ) if descendant is not None else None
                    )
                    if (
                        source is None
                        or source.get("status") != "replaced"
                        or source_identity is None
                        or descendant is None
                        or descendant_identity != target_identity
                    ):
                        return ReplaceResult(
                            outcome=RESULT_WRONG_STATE,
                            request_id=request_id,
                            descendant_request_id=(
                                int(descendant["id"]) if descendant else None
                            ),
                            error_message=(
                                "Replace tail no longer names the exact requested "
                                "source/descendant pair"
                            ),
                        )
                    assert source_identity is not None
                    assert descendant_identity is not None
                    with release_identity_locks(
                        self.db, (source_identity, descendant_identity),
                    ) as locks:
                        if not locks.acquired:
                            return ReplaceResult(
                                outcome=RESULT_TRANSIENT,
                                request_id=request_id,
                                descendant_request_id=int(descendant["id"]),
                                reason=REPLACE_REASON_LOCK_CONTENDED,
                                error_message="Replace association is busy; retry tail resumption",
                            )
                        # Re-read after the full lock union; a stale source or
                        # a sibling descendant is never safe to clean up.
                        current_source = self.db.get_request(request_id)
                        current_descendant = self.db.get_request_by_replaces_request_id(
                            request_id,
                        )
                        if (
                            current_source is None
                            or current_source.get("status") != "replaced"
                            or current_descendant is None
                            or ReleaseIdentity.from_strict_fields(
                                current_descendant.get("mb_release_id"),
                                current_descendant.get("discogs_release_id"),
                            ) != target_identity
                        ):
                            return ReplaceResult(
                                outcome=RESULT_WRONG_STATE,
                                request_id=request_id,
                                descendant_request_id=(
                                    int(current_descendant["id"])
                                    if current_descendant else None
                                ),
                                error_message="Replace pair changed while acquiring authority",
                            )
                        try:
                            beets = self.beets_db_factory()
                            try:
                                current_beets = resolve_current_for_request(
                                    beets, current_source,
                                )
                                library_db_path = beets.library_db_path
                                library_root = beets.library_root
                            finally:
                                beets.close()
                        except Exception as exc:  # noqa: BLE001 - typed retry
                            return ReplaceResult(
                                outcome=RESULT_TRANSIENT,
                                request_id=request_id,
                                descendant_request_id=int(current_descendant["id"]),
                                reason=REPLACE_REASON_POST_SUPERSEDE_PARTIAL,
                                error_message=(
                                    "Replace tail could not rederive current Beets "
                                    f"state: {type(exc).__name__}: {exc}"
                                ),
                            )
                        if isinstance(current_beets, CurrentBeetsAmbiguous) or current_beets is None:
                            return ReplaceResult(
                                outcome=RESULT_TRANSIENT,
                                request_id=request_id,
                                descendant_request_id=int(current_descendant["id"]),
                                reason=REPLACE_REASON_POST_SUPERSEDE_PARTIAL,
                                error_message="Replace tail current Beets authority is unavailable or ambiguous",
                            )
                        if isinstance(current_beets, CurrentBeetsUnique):
                            token.raise_if_cancelled()
                            request = BeetsDeleteRequest(
                                album_id=current_beets.album_id,
                                expected_release_id=current_beets.filed_identity.release_id,
                                library_db_path=library_db_path,
                                library_root=library_root,
                            )
                            if self.beets_delete_fn is run_beets_delete:
                                deleted = run_beets_delete(
                                    request,
                                    cancellation_token=token,
                                    owner_session_probe=lambda: bool(
                                        self.db._probe_owner_session(owner).live,
                                    ),
                                )
                            else:
                                deleted = self.beets_delete_fn(request)
                            token.raise_if_cancelled()
                            if not self.db._probe_owner_session(owner).live:
                                raise AdvisoryLockSessionLost(
                                    "Replace tail lost owner session after Beets acknowledgement"
                                )
                            if isinstance(deleted, BeetsDeleteFailed):
                                warnings.append(
                                    f"beets exact delete id:{current_beets.album_id} "
                                    f"failed {deleted.reason}: {deleted.detail}"
                                )
                        token.raise_if_cancelled()
                        summary = self.wrong_match_delete_fn(self.db, request_id)
                        token.raise_if_cancelled()
                        if not self.db._probe_owner_session(owner).live:
                            raise AdvisoryLockSessionLost(
                                "Replace tail lost owner session after wrong-match cleanup"
                            )
                        if summary.errors:
                            warnings.append(
                                f"wrong-matches cleanup reported {summary.errors} errors "
                                f"({summary.remaining} remaining)"
                            )
                        staging_dir = self.config.beets_staging_dir or None
                        artist = str(current_source.get("artist_name") or "")
                        title = str(current_source.get("album_title") or "")
                        if staging_dir and artist and title:
                            for auto_import in (True, False):
                                path = stage_to_ai_path(
                                    artist=artist, title=title,
                                    staging_dir=staging_dir, request_id=request_id,
                                    auto_import=auto_import,
                                )
                                if os.path.isdir(path):
                                    try:
                                        # ``rmtree`` cannot be made atomically
                                        # interruptible.  Its monotonic cleanup
                                        # is safe to retry, but a failed pass
                                        # remains an explicit resumable tail.
                                        token.raise_if_cancelled()
                                        shutil.rmtree(path)
                                    except FileNotFoundError:
                                        pass
                                    # Cleanup failure remains a resumable boundary.
                                    except Exception as exc:  # noqa: BLE001
                                        warnings.append(
                                            f"staging rmtree failed for {path}: "
                                            f"{type(exc).__name__}: {exc}"
                                        )
                                    token.raise_if_cancelled()
                                    if not self.db._probe_owner_session(owner).live:
                                        raise AdvisoryLockSessionLost(
                                            "Replace tail lost owner session after staging cleanup"
                                        )
                token.raise_if_cancelled()
                if not self.db._probe_owner_session(owner).live:
                    raise AdvisoryLockSessionLost(
                        "Replace tail lost owner session before search-plan readiness"
                    )
                plan = self.search_plan_service.generate_for_request(
                    int(descendant["id"]), regenerate=False,
                )
                token.raise_if_cancelled()
                if not self.db._probe_owner_session(owner).live:
                    raise AdvisoryLockSessionLost(
                        "Replace tail lost owner session after search-plan readiness"
                    )
                if plan.outcome not in {"success", "noop_active_plan_exists"}:
                    warnings.append(
                        f"search-plan generation returned {plan.outcome}"
                    )
        except (AdvisoryLockSessionLost, OwnerSessionLost):
            descendant = self.db.get_request_by_replaces_request_id(request_id)
            return ReplaceResult(
                outcome=RESULT_TRANSIENT,
                request_id=request_id,
                descendant_request_id=(int(descendant["id"]) if descendant else None),
                reason=REPLACE_REASON_POST_SUPERSEDE_PARTIAL,
                error_message="Replace tail lost authority; retry the same exact target",
                warnings=tuple(warnings),
            )
        descendant = self.db.get_request_by_replaces_request_id(request_id)
        assert descendant is not None
        if warnings:
            return ReplaceResult(
                outcome=RESULT_TRANSIENT,
                request_id=request_id,
                descendant_request_id=int(descendant["id"]),
                reason=REPLACE_REASON_POST_SUPERSEDE_PARTIAL,
                error_message="Replace tail is incomplete; retry the same exact target",
                warnings=tuple(warnings),
            )
        return ReplaceResult(
            outcome=RESULT_REPLACED,
            request_id=request_id,
            new_request_id=int(descendant["id"]),
        )

    def _finalize_replace(
        self,
        request_id: int,
        *,
        canonical_mbid: str,
        target_rg: str,
        target_data: TargetData,
        new_discogs_release_id: str | None,
    ) -> ReplaceResult:
        """Pin the exact PostgreSQL owner before any destructive lock.

        The pin starts the existing owner-session watchdog.  Every advisory
        lock and external delete below therefore belongs to one backend; a
        session death cannot be repaired by reconnecting mid-Replace.
        """
        token = CancellationToken()
        try:
            with self.db._pin_owner_session(token) as owner_session_identity:
                return self._finalize_replace_pinned(
                    request_id,
                    canonical_mbid=canonical_mbid,
                    target_rg=target_rg,
                    target_data=target_data,
                    new_discogs_release_id=new_discogs_release_id,
                    cancellation_token=token,
                    owner_session_identity=owner_session_identity,
                )
        except OwnerSessionLost as exc:
            raise AdvisoryLockSessionLost(
                "Replace could not retain its owner session"
            ) from exc

    def _finalize_replace_pinned(
        self,
        request_id: int,
        *,
        canonical_mbid: str,
        target_rg: str,
        target_data: TargetData,
        new_discogs_release_id: str | None,
        cancellation_token: CancellationToken,
        owner_session_identity: OwnerSessionIdentity,
    ) -> ReplaceResult:
        """Phases 1-5 — the mutation half, shared by the MB and Discogs
        arms once the target identity is resolved and validated.

        Acquires the IMPORT advisory lock, captures pre-supersede state,
        atomically supersedes the row (dual-writing
        ``new_discogs_release_id`` for the Discogs pathway; ``None`` for
        MB), runs non-fatal filesystem cleanup under the lock, then
        regenerates the search plan and fires the rescans OUTSIDE the lock.
        """
        # Phase 1 — acquire IMPORT advisory lock. See docs/advisory-locks.md.
        # We acquire BEFORE re-reading the source row so the importer
        # worker cannot finish and flip status
        # between our state capture and the supersede mutation.
        warnings: list[str] = []
        with self.db.advisory_lock(
            ADVISORY_LOCK_NAMESPACE_IMPORT, request_id
        ) as acquired:
            if not acquired:
                return ReplaceResult(
                    outcome=RESULT_TRANSIENT,
                    request_id=request_id,
                    reason=REPLACE_REASON_LOCK_CONTENDED,
                    error_message=(
                        f"importer is currently running for request "
                        f"{request_id}; retry in a few seconds"
                    ),
                )

            # Phase 2 — re-read source under the lock and capture
            # pre-supersede state. The lock guarantees no other writer
            # holds this row's IMPORT lock concurrently, so a fresh
            # ``get_request`` is sufficient — we don't need explicit
            # SELECT ... FOR UPDATE semantics here.
            source_locked = self.db.get_request(request_id)
            if source_locked is None:
                # Vanishingly rare — row was deleted between Phase 0
                # validation and lock acquire. Treat as not_found.
                return ReplaceResult(
                    outcome=RESULT_NOT_FOUND,
                    request_id=request_id,
                    error_message=(
                        f"request {request_id} disappeared after "
                        "advisory lock acquisition"
                    ),
                )
            processing_locked = _processing_locked_replace(
                source_locked,
                request_id,
            )
            if processing_locked is not None:
                return processing_locked
            # Re-check the double-click guard under the lock — if the
            # importer flipped status to ``replaced`` (it doesn't, but
            # defensively) or a concurrent Replace landed first, bail.
            if source_locked.get("status") == "replaced":
                descendant = self.db.get_request_by_replaces_request_id(
                    request_id
                )
                return ReplaceResult(
                    outcome=RESULT_WRONG_STATE,
                    request_id=request_id,
                    descendant_request_id=(
                        int(descendant["id"]) if descendant else None
                    ),
                    error_message=(
                        f"request {request_id} was replaced concurrently"
                    ),
                )
            old_artist = source_locked.get("artist_name") or ""
            old_title = source_locked.get("album_title") or ""
            old_status = source_locked.get("status")

            old_identity = ReleaseIdentity.from_strict_fields(
                source_locked.get("mb_release_id"),
                source_locked.get("discogs_release_id"),
            )
            if old_identity is None:
                return ReplaceResult(
                    outcome=RESULT_WRONG_STATE,
                    request_id=request_id,
                    reason=REPLACE_REASON_SOURCE_IDENTITY_INVALID,
                    error_message=(
                        f"request {request_id} has missing, malformed, or "
                        "conflicting exact release identity fields"
                    ),
                )
            target_identity = ReleaseIdentity.from_strict_fields(
                canonical_mbid, new_discogs_release_id,
            )
            if target_identity is None:
                return ReplaceResult(
                    outcome=RESULT_TARGET_INVALID,
                    request_id=request_id,
                    error_message="Replace target has no exact release identity",
                )

            # IMPORT is already held. Lock the complete before/after
            # association union before publishing the old-row removal and
            # target-row addition. See docs/advisory-locks.md.
            release_scope = release_identity_locks(
                self.db,
                (*acceptable_identities(source_locked), target_identity),
            )
            release_locks = release_scope.__enter__()
            if not release_locks.acquired:
                release_scope.__exit__(None, None, None)
                return ReplaceResult(
                    outcome=RESULT_TRANSIENT,
                    request_id=request_id,
                    reason=REPLACE_REASON_LOCK_CONTENDED,
                    error_message=(
                        "release association is currently changing; retry "
                        f"Replace for request {request_id}"
                    ),
                )
            try:
                source_confirmed = self.db.get_request(request_id)
                if source_confirmed is None:
                    return ReplaceResult(
                        outcome=RESULT_NOT_FOUND,
                        request_id=request_id,
                        error_message=(
                            f"request {request_id} disappeared while "
                            "acquiring release authority"
                        ),
                    )
                if source_confirmed.get("status") == "replaced":
                    return ReplaceResult(
                        outcome=RESULT_WRONG_STATE,
                        request_id=request_id,
                        error_message=(
                            f"request {request_id} was replaced concurrently"
                        ),
                    )
                if ReleaseIdentity.from_strict_fields(
                    source_confirmed.get("mb_release_id"),
                    source_confirmed.get("discogs_release_id"),
                ) != old_identity:
                    return ReplaceResult(
                        outcome=RESULT_WRONG_STATE,
                        request_id=request_id,
                        error_message=(
                            f"request {request_id} changed identity while "
                            "acquiring release authority"
                        ),
                    )
                target_existing = self.db.get_request_by_release_id(
                    canonical_mbid,
                )
                if (
                    target_existing is not None
                    and int(target_existing["id"]) != request_id
                ):
                    return ReplaceResult(
                        outcome=RESULT_TARGET_COLLISION_REQUEST,
                        request_id=request_id,
                        current_status=target_existing.get("status"),
                        error_message=(
                            f"target release {canonical_mbid} is already used "
                            f"by request {target_existing['id']} "
                            f"(status={target_existing.get('status')!r})"
                        ),
                    )
                source_locked = source_confirmed
                try:
                    beets_db = self.beets_db_factory()
                    try:
                        # Union (#1059). ``current_album_path`` below is what
                        # Replace uses to clean up the superseded album; an
                        # acquisition-only resolve returns Missing for a merged
                        # request whose files Beets holds under the survivor, so
                        # Replace would supersede and leave the old album on
                        # disk — manufacturing exactly the orphan class this
                        # issue exists to clear.
                        current_beets = resolve_current_for_request(
                            beets_db, source_locked,
                        )
                        if current_beets is None:
                            # Unreachable — ``old_identity`` was already proven
                            # above. Raised rather than resolved acquisition-only:
                            # the caller converts it to the typed
                            # ``current_beets_unavailable`` refusal, and an
                            # authority failure must not become a resolution on
                            # a path that supersedes and deletes.
                            raise RuntimeError(
                                "current Beets authority omitted the request's "
                                "acceptable release identities"
                            )
                        current_library_db_path = beets_db.library_db_path
                        current_library_root = beets_db.library_root
                    finally:
                        beets_db.close()
                except Exception as exc:  # noqa: BLE001 -- typed zero-mutation result
                    return ReplaceResult(
                        outcome=RESULT_WRONG_STATE,
                        request_id=request_id,
                        reason=REPLACE_REASON_CURRENT_BEETS_UNAVAILABLE,
                        error_message=(
                            "current Beets resolution failed before Replace: "
                            f"{type(exc).__name__}: {exc}"
                        ),
                    )
                if isinstance(current_beets, CurrentBeetsAmbiguous):
                    return ReplaceResult(
                        outcome=RESULT_WRONG_STATE,
                        request_id=request_id,
                        reason=REPLACE_REASON_CURRENT_BEETS_AMBIGUOUS,
                        error_message=(
                            f"current Beets authority for {old_identity.release_id} "
                            f"is ambiguous ({current_beets.reason}; album ids "
                            f"{list(current_beets.album_ids)})"
                        ),
                    )
                current_album_path = (
                    current_beets.album_path
                    if isinstance(current_beets, CurrentBeetsUnique)
                    else None
                )

                # Phase 3 — DB transaction.
                try:
                    new_request_id = self.db.supersede_request_mbid(
                        request_id,
                        new_mb_release_id=canonical_mbid,
                        new_mb_release_group_id=target_rg,
                        new_mb_artist_id=target_data.get("artist_id"),
                        new_artist_name=target_data.get("artist_name") or "",
                        new_album_title=target_data.get("title") or "",
                        new_year=target_data.get("year"),
                        new_country=target_data.get("country"),
                        new_tracks=list(target_data.get("tracks") or []),
                        new_discogs_release_id=new_discogs_release_id,
                    )
                except MbidCollisionError as exc:
                    return ReplaceResult(
                        outcome=RESULT_TARGET_COLLISION_REQUEST,
                        request_id=request_id,
                        error_message=(
                            f"target MBID collision on supersede: {exc}"
                        ),
                    )
                except SupersedeRaceError as exc:
                    # A concurrent Replace (double-click) landed first
                    # while we held the lock. The descendant row already
                    # exists — surface a deep-link rather than telling the
                    # operator to retry; retrying a race that has already
                    # succeeded is misleading. Mirrors the Phase 0 step 1a
                    # early-exit shape (RESULT_WRONG_STATE +
                    # descendant_request_id).
                    descendant = self.db.get_request_by_replaces_request_id(
                        request_id
                    )
                    return ReplaceResult(
                        outcome=RESULT_WRONG_STATE,
                        request_id=request_id,
                        descendant_request_id=(
                            int(descendant["id"]) if descendant else None
                        ),
                        error_message=(
                            f"supersede race on request {request_id}: {exc}"
                        ),
                    )

                cancellation_token.raise_if_cancelled()
                if not self.db._probe_owner_session(
                    owner_session_identity,
                ).live:
                    raise AdvisoryLockSessionLost(
                        "Replace lost owner session immediately after supersede"
                    )

                # Phase 4 — filesystem cleanup (non-fatal). Keyed on the fresh
                # exact Beets album PK — never on request status. "wanted" does not
                # mean "nothing on disk": library-backfill rows (2026-06-04)
                # track pre-existing installs while still wanted, and Replace
                # REPLACES — the old pressing's install is displaced whenever
                # it resolves in beets (the Passenger regression, 2026-07-18).
                # Missing current Beets authority is a safe no-op.
                if isinstance(current_beets, CurrentBeetsUnique):
                    cancellation_token.raise_if_cancelled()
                    try:
                        delete_request = BeetsDeleteRequest(
                            album_id=current_beets.album_id,
                            # FILED, not requested (#1059). The delete child
                            # refuses a mismatch against the album's own
                            # mb_albumid, and the supersede has already
                            # committed — so the acquisition id here leaves the
                            # old album on disk, which is the orphan class this
                            # issue exists to clear.
                            expected_release_id=(
                                current_beets.filed_identity.release_id
                            ),
                            library_db_path=current_library_db_path,
                            library_root=current_library_root,
                        )
                        if self.beets_delete_fn is run_beets_delete:
                            delete_outcome = run_beets_delete(
                                delete_request,
                                cancellation_token=cancellation_token,
                                owner_session_probe=lambda: bool(
                                    self.db._probe_owner_session(
                                        owner_session_identity,
                                    ).live,
                                ),
                            )
                        else:
                            delete_outcome = self.beets_delete_fn(delete_request)
                        cancellation_token.raise_if_cancelled()
                        if not self.db._probe_owner_session(
                            owner_session_identity,
                        ).live:
                            raise AdvisoryLockSessionLost(
                                "Replace lost owner session after Beets acknowledgement"
                            )
                        if isinstance(delete_outcome, BeetsDeleteFailed):
                            warnings.append(
                                f"beets exact delete id:{current_beets.album_id} "
                                f"failed {delete_outcome.reason}: "
                                f"{delete_outcome.detail}"
                            )
                    except AdvisoryLockSessionLost:
                        raise
                    except Exception as exc:  # noqa: BLE001 - boundary converts or isolates collaborator failures
                        warnings.append(
                            f"beets removal raised "
                            f"{type(exc).__name__}: {exc}"
                        )

                try:
                    cancellation_token.raise_if_cancelled()
                    wm_summary = self.wrong_match_delete_fn(self.db, request_id)
                    cancellation_token.raise_if_cancelled()
                    if not self.db._probe_owner_session(
                        owner_session_identity,
                    ).live:
                        raise AdvisoryLockSessionLost(
                            "Replace lost owner session after wrong-match cleanup"
                        )
                    if wm_summary.errors:
                        warnings.append(
                            f"wrong-matches cleanup reported "
                            f"{wm_summary.errors} errors "
                            f"({wm_summary.remaining} remaining)"
                        )
                except AdvisoryLockSessionLost:
                    # Do not turn loss of the association-lock session into a
                    # successful Replace warning.  The public boundary below
                    # finds the committed descendant and refuses a retry.
                    raise
                except Exception as exc:  # noqa: BLE001 - boundary converts or isolates collaborator failures
                    warnings.append(
                        f"wrong-matches cleanup raised "
                        f"{type(exc).__name__}: {exc}"
                    )

                if old_status == "downloading":
                    warnings.append(
                        f"request {request_id} was downloading; in-flight "
                        "slskd transfers are not cancelled and staging "
                        "cleanup was skipped (see issue #278)"
                    )
                else:
                    # CratediggerConfig always has the field — empty
                    # string when unconfigured. Coerce to None so the
                    # downstream guard reads cleanly.
                    staging_dir = self.config.beets_staging_dir or None
                    if staging_dir and old_artist and old_title:
                        for auto_import in (True, False):
                            path = stage_to_ai_path(
                                artist=old_artist,
                                title=old_title,
                                staging_dir=staging_dir,
                                request_id=request_id,
                                auto_import=auto_import,
                            )
                            if not os.path.isdir(path):
                                continue
                            try:
                                # ``rmtree`` cannot be made atomically
                                # interruptible. Its monotonic path cleanup is
                                # safe to retry, but cancellation is checked on
                                # both sides so we never claim the ambiguous
                                # span completed under lost authority.
                                cancellation_token.raise_if_cancelled()
                                shutil.rmtree(path)
                                cancellation_token.raise_if_cancelled()
                                if not self.db._probe_owner_session(
                                    owner_session_identity,
                                ).live:
                                    raise AdvisoryLockSessionLost(
                                        "Replace lost owner session after staging cleanup"
                                    )
                            except FileNotFoundError:
                                pass
                            except AdvisoryLockSessionLost:
                                raise
                            except Exception as exc:  # noqa: BLE001 - boundary converts or isolates collaborator failures
                                warnings.append(
                                    f"staging rmtree failed for {path}: "
                                    f"{type(exc).__name__}: {exc}"
                                )
            finally:
                release_scope.__exit__(*sys.exc_info())

        # Phase 5 — search plan + rescans (OUTSIDE the advisory lock).
        # Rescans each carry their own ~10s timeout; holding the IMPORT
        # lock across them buys nothing because the new request's
        # ``active_plan_id`` is NULL until the search plan is generated,
        # and the importer worker only acquires the per-request lock when
        # it has work to do. Releasing early caps lock-hold at fs
        # cleanup (sub-second) rather than ~30s worst case.
        try:
            cancellation_token.raise_if_cancelled()
            if not self.db._probe_owner_session(owner_session_identity).live:
                raise AdvisoryLockSessionLost(
                    "Replace lost owner session before search-plan generation"
                )
            self.search_plan_service.generate_for_request(
                new_request_id, regenerate=False,
            )
            cancellation_token.raise_if_cancelled()
            if not self.db._probe_owner_session(owner_session_identity).live:
                raise AdvisoryLockSessionLost(
                    "Replace lost owner session after search-plan generation"
                )
        except AdvisoryLockSessionLost:
            raise
        except Exception as exc:  # noqa: BLE001 - boundary converts or isolates collaborator failures
            warnings.append(
                f"search-plan generation failed for new request "
                f"{new_request_id}: {type(exc).__name__}: {exc}"
            )

        try:
            trigger_plex_scan(
                self.config, imported_path=current_album_path
            )
        except Exception as exc:  # noqa: BLE001 - boundary converts or isolates collaborator failures
            warnings.append(
                f"plex rescan failed: {type(exc).__name__}: {exc}"
            )
        try:
            trigger_jellyfin_scan(
                self.config, imported_path=current_album_path
            )
        except Exception as exc:  # noqa: BLE001 - boundary converts or isolates collaborator failures
            warnings.append(
                f"jellyfin rescan failed: {type(exc).__name__}: {exc}"
            )

        logger.info(
            "Replace: success request_id=%d new_request_id=%d warnings=%d",
            request_id, new_request_id, len(warnings),
        )
        for w in warnings:
            logger.warning("Replace: warning request_id=%d: %s", request_id, w)
        return ReplaceResult(
            outcome=RESULT_REPLACED,
            request_id=request_id,
            new_request_id=new_request_id,
            warnings=tuple(warnings),
        )
