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
Discogs master (numeric id in ``mb_release_group_id``, KTD-1). A target on
the OTHER pathway is accepted only under the operator's explicit
``cross_pathway`` opt-in (issue #1366): the Browse tab pairs an MB release
group with a Discogs master as one album, that pairing surfaces the offer,
and the operator's confirmation is the identity authority — there is no
shared group across pathways for the service to compare, so the
group-mismatch gate does not apply and the source's own group is never
consulted. Everything else (target resolution, collisions, canonical
redirects, the supersede itself) is the same machinery the two
same-pathway arms use.

See ``docs/plans/2026-07-04-001-feat-discogs-pathway-replace-plan.md`` and
``docs/plans/2026-05-18-001-feat-replace-operator-action-plan.md`` for the
full design; issue #1366 supersedes that plan's R4 / AE2.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import socket
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol, runtime_checkable
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
    BeetsDeleteFn,
    BeetsDeleteRequest,
    run_beets_delete,
)
from lib.config import CratediggerConfig
from lib.pipeline_db import (
    ADVISORY_LOCK_NAMESPACE_IMPORT,
    MbidCollisionError,
    SupersedeRaceError,
)
from lib.processing_paths import stage_to_ai_path
from lib.release_identity import (
    ReleaseIdentity,
    detect_release_source,
    normalize_release_id,
)
from lib.release_payload import (
    release_int_or_none,
    release_str,
    release_str_or_none,
    release_tracks,
)
from lib.replace_status import (
    REPLACE_REASON_CROSS_PATHWAY_TARGET,
    REPLACE_REASON_CURRENT_BEETS_AMBIGUOUS,
    REPLACE_REASON_CURRENT_BEETS_UNAVAILABLE,
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
from lib.search_plan_service import SearchPlanDB, SearchPlanService
from lib.util import (
    trigger_jellyfin_scan,
    trigger_plex_scan,
)
from lib.wrong_match_delete_service import (
    WrongMatchDeleteDB,
    delete_wrong_match_group,
)

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
        new_tracks: list[dict[str, object]],
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


@dataclass(frozen=True)
class _ResolvedTarget:
    """A target id the mirror resolved and the service pre-validated.

    Internal to the service (never crosses JSON, so a dataclass). Produced
    by ``_resolve_mb_target`` / ``_resolve_discogs_target`` — the shared
    collision-precheck + fresh-lookup + payload/group validation that the
    MB arm, the Discogs arm, and the cross-pathway arm all perform before
    their own pathway-specific checks.
    """

    canonical_id: str
    group_id: str | None
    data: dict[str, object]


# Type aliases for the injectable dependencies.
MBLookup = Callable[..., dict[str, object]]
"""Signature: ``mb_lookup(mbid, *, fresh: bool=False) -> dict[str, object]``
(the one payload shape both mirrors emit; fields are read through
``lib.release_payload``). The default is ``web.mb.get_release``; tests
inject a fake."""

DiscogsLookup = Callable[..., dict[str, object]]
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

    def close(self) -> None: ...


BeetsDBFactory = Callable[[], ReplaceBeetsDB]
"""Zero-arg callable returning a ``BeetsDB`` instance. Default uses
``lib.beets_db.BeetsDB`` against the configured library path."""


def _default_mb_lookup(mbid: str, *, fresh: bool = False) -> dict[str, object]:
    """Default MB-mirror lookup. Imported lazily so the service module
    doesn't pull in ``web.mb``'s urllib transport at import time."""
    from web.mb import get_release
    return get_release(mbid, fresh=fresh)


def _default_discogs_lookup(
    release_id: int, *, fresh: bool = False,
) -> dict[str, object]:
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
        slskd: object | None = None,
        beets_db_factory: BeetsDBFactory | None = None,
        mb_lookup: MBLookup | None = None,
        discogs_lookup: DiscogsLookup | None = None,
        search_plan_service: SearchPlanService | None = None,
        beets_delete_fn: BeetsDeleteFn | None = None,
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

    def replace_request_mbid(
        self,
        request_id: int,
        *,
        target_mb_release_id: str,
        cross_pathway: bool = False,
    ) -> ReplaceResult:
        """Supersede ``request_id`` with a new row at ``target_mb_release_id``.

        ``cross_pathway`` is the operator's explicit assertion that a target
        on the other pathway (a Discogs release for an MB source, or an MB
        release for a Discogs source) is the same album (issue #1366).
        Without it a cross-pathway target is refused; with it the
        cross-pathway arm runs. It is inert for a same-pathway target.

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
            "Replace: request_id=%d target_mb_release_id=%s cross_pathway=%s",
            request_id, target_mb_release_id, cross_pathway,
        )
        # The target's letter case is not identity (issue #1382 item 3):
        # canonicalise once here so the same-as-current check, the
        # collision pre-check, the mirror lookup and the supersede write
        # all see one id. The shape refusal below quotes the target as the
        # operator typed it, padding and all.
        typed_target = target_mb_release_id
        target_mb_release_id = (
            normalize_release_id(target_mb_release_id) or target_mb_release_id
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
        # audit row is not a valid source for another Replace.
        if source.get("status") == "replaced":
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

        # Pathway-aware target gate. ``detect_release_source`` is the single
        # authority for each id's pathway (KTD-2). A target that parses as
        # neither shape is invalid outright. A target on the OTHER pathway
        # is valid only under the operator's explicit ``cross_pathway``
        # opt-in (issue #1366, superseding the #282 plan's R4 / AE2 scope
        # boundary): the Browse tab pairs an MB release group with a
        # Discogs master as one album, that pairing surfaces the offer,
        # and the operator's confirmation is the identity authority — no
        # shared group exists across pathways for the service to compare.
        # Same-pathway targets take the original arms untouched, whatever
        # the flag says.
        source_source = source_identity.source
        target_source = detect_release_source(target_mb_release_id)
        if target_source not in ("musicbrainz", "discogs"):
            return ReplaceResult(
                outcome=RESULT_TARGET_INVALID,
                request_id=request_id,
                error_message=(
                    f"target {typed_target!r} is neither an MB "
                    "release UUID nor a Discogs release id"
                ),
                reason=REPLACE_REASON_CROSS_PATHWAY_TARGET,
            )
        if target_source != source_source:
            if not cross_pathway:
                return ReplaceResult(
                    outcome=RESULT_TARGET_INVALID,
                    request_id=request_id,
                    error_message=(
                        f"target {typed_target!r} ({target_source}) "
                        f"is on the other pathway from source "
                        f"({source_source}); pass cross_pathway to supersede "
                        "across pathways when the two are the same album"
                    ),
                    reason=REPLACE_REASON_CROSS_PATHWAY_TARGET,
                )
            return self._replace_cross_pathway_target(
                request_id,
                source,
                source_mbid,
                target_mb_release_id,
                target_source,
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
            # ``release_group_id`` is None when the mirror doesn't have one.
            source_rg = release_str_or_none(src_data, "release_group_id")
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

        # Collision pre-check, fresh MB lookup, payload and release-group
        # validation — shared with the cross-pathway arm.
        resolved = self._resolve_mb_target(request_id, target_mb_release_id)
        if isinstance(resolved, ReplaceResult):
            return resolved

        if resolved.group_id != source_rg:
            return ReplaceResult(
                outcome=RESULT_TARGET_RELEASE_GROUP_MISMATCH,
                request_id=request_id,
                error_message=(
                    f"target release group {resolved.group_id} does not "
                    f"match source release group {source_rg}"
                ),
            )

        # Handle MB 301 redirect: if the canonical MBID differs from
        # what the operator requested, re-check collision against the
        # canonical and (defensively) against the source.
        redirect = self._canonical_redirect_collision(
            request_id,
            source,
            requested=target_mb_release_id,
            canonical=resolved.canonical_id,
            source_id=source_mbid,
            lookup_existing=self.db.get_request_by_mb_release_id,
            label=f"target MBID {target_mb_release_id}",
            source_word="MBID",
        )
        if redirect is not None:
            return redirect

        return self._finalize_replace(
            request_id,
            canonical_mbid=resolved.canonical_id,
            target_rg=resolved.group_id,
            target_data=resolved.data,
            new_discogs_release_id=None,
        )

    def _resolve_mb_target(
        self,
        request_id: int,
        target_mbid: str,
    ) -> _ResolvedTarget | ReplaceResult:
        """Collision pre-check, fresh MB lookup, empty-payload and
        release-group validation for an MB target, in the MB arm's
        original order. Shared by the MB arm and the cross-pathway arm;
        the caller adds whatever pathway-specific gate applies (the
        release-group match, for same-pathway) and the canonical-redirect
        re-check."""
        existing = self.db.get_request_by_mb_release_id(target_mbid)
        if existing is not None and int(existing["id"]) != request_id:
            return ReplaceResult(
                outcome=RESULT_TARGET_COLLISION_REQUEST,
                request_id=request_id,
                current_status=existing.get("status"),
                error_message=(
                    f"target MBID {target_mbid} is already used "
                    f"by request {existing['id']} "
                    f"(status={existing.get('status')!r})"
                ),
            )

        target_data, err = self._mb_lookup_or_error(
            target_mbid,
            request_id=request_id,
            detail_context=f"target MBID {target_mbid}",
        )
        if err is not None:
            return err
        assert target_data is not None

        if not target_data:
            return ReplaceResult(
                outcome=RESULT_TARGET_INVALID,
                request_id=request_id,
                error_message=(
                    f"target MBID {target_mbid} returned empty "
                    "payload from MB mirror"
                ),
                reason=REPLACE_REASON_UNRESOLVABLE_TARGET,
            )

        # The mirror's canonical is normalised exactly as the typed target
        # was (issue #1382 item 3). MusicBrainz serves lowercase UUIDs, so
        # this is fail-closed legislation for the external boundary: were
        # an uppercase ``id`` ever to arrive, it must neither read as a
        # redirect nor be written as a second identity.
        raw_canonical = release_str_or_none(target_data, "id") or target_mbid
        canonical_mbid = normalize_release_id(raw_canonical) or raw_canonical
        if detect_release_source(canonical_mbid) != "musicbrainz":
            # The mirror canonicalised the picked id onto something that is
            # not an MB release UUID. Nothing downstream can trust that
            # identity — refuse before it reaches the collision re-check
            # or the supersede write.
            return ReplaceResult(
                outcome=RESULT_TARGET_INVALID,
                request_id=request_id,
                error_message=(
                    f"target MBID {target_mbid} canonicalised to "
                    f"{canonical_mbid!r}, which is not an MB release UUID"
                ),
                reason=REPLACE_REASON_UNRESOLVABLE_TARGET,
            )
        target_rg = release_str_or_none(target_data, "release_group_id")
        if not target_rg:
            return ReplaceResult(
                outcome=RESULT_TARGET_INVALID,
                request_id=request_id,
                error_message=(
                    f"target MBID {target_mbid} resolved with "
                    "no release_group_id"
                ),
                reason=REPLACE_REASON_TARGET_NO_RELEASE_GROUP,
            )
        return _ResolvedTarget(
            canonical_id=canonical_mbid,
            group_id=target_rg,
            data=target_data,
        )

    def _canonical_redirect_collision(
        self,
        request_id: int,
        source: AlbumRequestRow,
        *,
        requested: str,
        canonical: str,
        source_id: str,
        lookup_existing: Callable[[str], AlbumRequestRow | None],
        label: str,
        source_word: str,
    ) -> ReplaceResult | None:
        """The canonical-redirect re-check every arm runs after its own
        gates: when the mirror canonicalised the requested id onto a
        different one, that canonical id must not be the source's own
        current id nor held by another active request. ``lookup_existing``
        is the pathway's collision lookup (``get_request_by_mb_release_id``
        for MB, the identity-aware ``get_request_by_release_id`` for
        Discogs, KTD-6); ``label`` / ``source_word`` keep each arm's
        original wording."""
        if canonical == requested:
            return None
        if canonical == source_id:
            return ReplaceResult(
                outcome=RESULT_TARGET_COLLISION_REQUEST,
                request_id=request_id,
                current_status=source.get("status"),
                error_message=(
                    f"{label} redirects to canonical {canonical} which is "
                    f"the source's current {source_word}"
                ),
            )
        existing_canon = lookup_existing(canonical)
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
                    f"{canonical} held by request "
                    f"{existing_canon['id']} "
                    f"(status={existing_canon.get('status')!r})"
                ),
            )
        return None

    def _replace_cross_pathway_target(
        self,
        request_id: int,
        source: AlbumRequestRow,
        source_mbid: str,
        target_mb_release_id: str,
        target_source: str,
    ) -> ReplaceResult:
        """Cross-pathway arm (issue #1366) — reached only under the
        operator's explicit ``cross_pathway`` opt-in with a target on the
        other pathway from the source.

        No shared release group exists across pathways, so there is no
        group-mismatch gate and nothing about the source's group bears on
        the target: the source-pathway lookup is never issued (a legacy
        source with no persisted group crosses without a backfill). The
        target pathway's own resolution runs exactly as in its
        same-pathway arm — collision pre-check, fresh lookup, empty-payload
        check, and the canonical-redirect re-check — with one relaxation:
        a Discogs target may be masterless. Masterless releases pair in
        the Browse tab too, and the same-pathway arm only demanded a
        master to anchor siblings; the new row is born with no master,
        exactly as the add flow writes one. An MB target still owes its
        release group. The resolvers refuse a canonical id that is not on
        the target's pathway, so the redirect re-check's "canonical is the
        source's own id" branch cannot fire here (the two ids never share
        a shape); its wording is kept pathway-neutral regardless. The
        supersede then dual-writes the new row in ITS pathway's shape
        (R5-R7), linked by the ordinary supersede link.
        """
        if target_source == "musicbrainz":
            resolved = self._resolve_mb_target(request_id, target_mb_release_id)
            if isinstance(resolved, ReplaceResult):
                return resolved
            redirect = self._canonical_redirect_collision(
                request_id,
                source,
                requested=target_mb_release_id,
                canonical=resolved.canonical_id,
                source_id=source_mbid,
                lookup_existing=self.db.get_request_by_mb_release_id,
                label=f"target MBID {target_mb_release_id}",
                source_word="id",
            )
            new_discogs_release_id: str | None = None
        else:
            resolved = self._resolve_discogs_target(
                request_id, target_mb_release_id, require_master=False,
            )
            if isinstance(resolved, ReplaceResult):
                return resolved
            redirect = self._canonical_redirect_collision(
                request_id,
                source,
                requested=normalize_release_id(target_mb_release_id),
                canonical=resolved.canonical_id,
                source_id=normalize_release_id(source_mbid),
                lookup_existing=self.db.get_request_by_release_id,
                label=f"target Discogs id {target_mb_release_id}",
                source_word="id",
            )
            new_discogs_release_id = resolved.canonical_id
        if redirect is not None:
            return redirect

        logger.info(
            "Replace: cross-pathway request_id=%d source=%s target=%s (%s)",
            request_id, source_mbid, resolved.canonical_id, target_source,
        )
        return self._finalize_replace(
            request_id,
            canonical_mbid=resolved.canonical_id,
            target_rg=resolved.group_id,
            target_data=resolved.data,
            new_discogs_release_id=new_discogs_release_id,
        )

    def _mb_lookup_or_error(
        self,
        mbid: str,
        *,
        request_id: int,
        detail_context: str,
    ) -> tuple[dict[str, object] | None, ReplaceResult | None]:
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
        source: AlbumRequestRow,
        source_mbid: str,
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
            source_master = release_str_or_none(src_data, "release_group_id")
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

        # Collision pre-check, fresh Discogs lookup, payload and master
        # validation — shared with the cross-pathway arm, which alone may
        # relax the master requirement.
        resolved = self._resolve_discogs_target(
            request_id, target_mb_release_id, require_master=True,
        )
        if isinstance(resolved, ReplaceResult):
            return resolved

        if resolved.group_id != source_master:
            return ReplaceResult(
                outcome=RESULT_TARGET_RELEASE_GROUP_MISMATCH,
                request_id=request_id,
                error_message=(
                    f"target master {resolved.group_id} does not match "
                    f"source master {source_master}"
                ),
            )

        # Canonical-redirect re-check (mirror the MB arm): if the mirror
        # returned a different canonical id, re-check collision against it
        # and (defensively) against the source.
        redirect = self._canonical_redirect_collision(
            request_id,
            source,
            requested=normalize_release_id(target_mb_release_id),
            canonical=resolved.canonical_id,
            source_id=normalize_release_id(source_mbid),
            lookup_existing=self.db.get_request_by_release_id,
            label=f"target Discogs id {target_mb_release_id}",
            source_word="id",
        )
        if redirect is not None:
            return redirect

        return self._finalize_replace(
            request_id,
            canonical_mbid=resolved.canonical_id,
            target_rg=resolved.group_id,
            target_data=resolved.data,
            new_discogs_release_id=resolved.canonical_id,
        )

    def _resolve_discogs_target(
        self,
        request_id: int,
        target_id: str,
        *,
        require_master: bool,
    ) -> _ResolvedTarget | ReplaceResult:
        """Collision pre-check (identity-aware, KTD-6), fresh Discogs
        lookup, empty-payload and master validation for a Discogs target,
        in the Discogs arm's original order. Shared by the Discogs arm
        (``require_master=True`` — siblings anchor on the master) and the
        cross-pathway arm (``require_master=False`` — a masterless release
        is a legal target there; see ``_replace_cross_pathway_target``)."""
        normalized_target = normalize_release_id(target_id)
        target_id_num = int(normalized_target)

        existing = self.db.get_request_by_release_id(target_id)
        if existing is not None and int(existing["id"]) != request_id:
            return ReplaceResult(
                outcome=RESULT_TARGET_COLLISION_REQUEST,
                request_id=request_id,
                current_status=existing.get("status"),
                error_message=(
                    f"target Discogs id {target_id} is already "
                    f"used by request {existing['id']} "
                    f"(status={existing.get('status')!r})"
                ),
            )

        target_data, err = self._discogs_lookup_or_error(
            target_id_num,
            request_id=request_id,
            detail_context=f"target Discogs id {target_id}",
        )
        if err is not None:
            return err
        assert target_data is not None

        if not target_data:
            return ReplaceResult(
                outcome=RESULT_TARGET_INVALID,
                request_id=request_id,
                error_message=(
                    f"target Discogs id {target_id} returned "
                    "empty payload from the mirror"
                ),
                reason=REPLACE_REASON_UNRESOLVABLE_TARGET,
            )

        raw_canonical = release_str_or_none(target_data, "id") or target_id
        canonical_id = normalize_release_id(raw_canonical) or raw_canonical
        if detect_release_source(canonical_id) != "discogs":
            # Same guard as the MB resolver. This also refuses the mirror
            # Struct's ``id`` default of ``0`` (``"0"`` normalizes to no
            # release at all), which would otherwise be written as the new
            # row's exact identity.
            return ReplaceResult(
                outcome=RESULT_TARGET_INVALID,
                request_id=request_id,
                error_message=(
                    f"target Discogs id {target_id} canonicalised to "
                    f"{canonical_id!r}, which is not a Discogs release id"
                ),
                reason=REPLACE_REASON_UNRESOLVABLE_TARGET,
            )
        target_master = release_str_or_none(target_data, "release_group_id")
        if require_master and not target_master:
            return ReplaceResult(
                outcome=RESULT_TARGET_INVALID,
                request_id=request_id,
                error_message=(
                    f"target Discogs id {target_id} resolved "
                    "with no master"
                ),
                reason=REPLACE_REASON_TARGET_NO_RELEASE_GROUP,
            )
        return _ResolvedTarget(
            canonical_id=canonical_id,
            group_id=target_master,
            data=target_data,
        )

    def _discogs_lookup_or_error(
        self,
        release_id_num: int,
        *,
        request_id: int,
        detail_context: str,
    ) -> tuple[dict[str, object] | None, ReplaceResult | None]:
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

    def _finalize_replace(
        self,
        request_id: int,
        *,
        canonical_mbid: str,
        target_rg: str | None,
        target_data: dict[str, object],
        new_discogs_release_id: str | None,
    ) -> ReplaceResult:
        """Phases 1-5 — the mutation half, shared by the MB, Discogs, and
        cross-pathway arms once the target identity is resolved and
        validated. ``target_rg`` is ``None`` only for a masterless Discogs
        target reached through the cross-pathway arm.

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
                    outcome=RESULT_WRONG_STATE,
                    request_id=request_id,
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
            try:
                beets_db = self.beets_db_factory()
                try:
                    current_beets = beets_db.resolve_current_release(old_identity)
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
                    new_mb_artist_id=release_str_or_none(
                        target_data, "artist_id",
                    ),
                    new_artist_name=release_str(target_data, "artist_name"),
                    new_album_title=release_str(target_data, "title"),
                    new_year=release_int_or_none(target_data, "year"),
                    new_country=release_str_or_none(target_data, "country"),
                    new_tracks=release_tracks(target_data),
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

            # Phase 4 — filesystem cleanup (non-fatal). Keyed on the fresh
            # exact Beets album PK — never on request status. "wanted" does not
            # mean "nothing on disk": library-backfill rows (2026-06-04)
            # track pre-existing installs while still wanted, and Replace
            # REPLACES — the old pressing's install is displaced whenever
            # it resolves in beets (the Passenger regression, 2026-07-18).
            # Missing current Beets authority is a safe no-op.
            if isinstance(current_beets, CurrentBeetsUnique):
                try:
                    delete_outcome = self.beets_delete_fn(BeetsDeleteRequest(
                        album_id=current_beets.album_id,
                        expected_release_id=old_identity.release_id,
                        library_db_path=current_library_db_path,
                        library_root=current_library_root,
                    ))
                    if isinstance(delete_outcome, BeetsDeleteFailed):
                        warnings.append(
                            f"beets exact delete id:{current_beets.album_id} "
                            f"failed {delete_outcome.reason}: "
                            f"{delete_outcome.detail}"
                        )
                except Exception as exc:  # noqa: BLE001 - boundary converts or isolates collaborator failures
                    warnings.append(
                        f"beets removal raised "
                        f"{type(exc).__name__}: {exc}"
                    )

            try:
                wm_summary = delete_wrong_match_group(self.db, request_id)
                if not wm_summary.success:
                    # Skipped-without-error is just as silent a failure as
                    # an error: sources and their pointers survive while
                    # Replace claims a clean supersede (issue #1063).
                    # ``success`` already requires ``remaining == 0`` on
                    # both of its branches, so it is the whole condition.
                    warnings.append(
                        f"wrong-matches cleanup did not complete: "
                        f"{wm_summary.errors} errors, "
                        f"{wm_summary.skipped} skipped, "
                        f"{wm_summary.unavailable} unavailable, "
                        f"{wm_summary.remaining} remaining"
                    )
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
                            shutil.rmtree(path)
                        except FileNotFoundError:
                            pass
                        except Exception as exc:  # noqa: BLE001 - boundary converts or isolates collaborator failures
                            warnings.append(
                                f"staging rmtree failed for {path}: "
                                f"{type(exc).__name__}: {exc}"
                            )

        # Phase 5 — search plan + rescans (OUTSIDE the advisory lock).
        # Rescans each carry their own ~10s timeout; holding the IMPORT
        # lock across them buys nothing because the new request's
        # ``active_plan_id`` is NULL until the search plan is generated,
        # and the importer worker only acquires the per-request lock when
        # it has work to do. Releasing early caps lock-hold at fs
        # cleanup (sub-second) rather than ~30s worst case.
        try:
            self.search_plan_service.generate_for_request(
                new_request_id, regenerate=False,
            )
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
