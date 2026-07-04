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
from dataclasses import dataclass, field
from typing import Any, Callable, Protocol, runtime_checkable
from urllib.error import URLError


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

from lib.config import CratediggerConfig
from lib.release_identity import detect_release_source, normalize_release_id
from lib.pipeline_db import (
    ADVISORY_LOCK_NAMESPACE_IMPORT,
    MbidCollisionError,
    SupersedeRaceError,
)
from lib.processing_paths import stage_to_ai_path
from lib.release_cleanup import ReleaseCleanupDB, remove_and_reset_release
from lib.search_plan_service import SearchPlanDB, SearchPlanService
from lib.util import (
    trigger_jellyfin_scan,
    trigger_meelo_scan,
    trigger_plex_scan,
)
from lib.wrong_match_delete_service import (
    WrongMatchDeleteDB,
    delete_wrong_match_group,
)

logger = logging.getLogger(__name__)


@runtime_checkable
class MbidReplaceDB(
    WrongMatchDeleteDB, ReleaseCleanupDB, SearchPlanDB, Protocol,
):
    """The PipelineDB surface the Replace action uses (#409).

    Extends the protocols of everything the handle is forwarded into:
    ``delete_wrong_match_group``, ``remove_and_reset_release``, and the
    constructor-built ``SearchPlanService``. Parity tests live in
    ``tests/test_mbid_replace_service.py``.
    """

    def get_request_by_mb_release_id(
        self, mb_release_id: str,
    ) -> dict[str, Any] | None: ...

    def get_request_by_release_id(
        self, release_id: object | None,
    ) -> dict[str, Any] | None: ...

    def get_request_by_replaces_request_id(
        self, replaced_id: int,
    ) -> dict[str, Any] | None: ...

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


# Result outcome constants.
RESULT_REPLACED = "replaced"
RESULT_NOT_FOUND = "not_found"
RESULT_WRONG_STATE = "wrong_state"
RESULT_TARGET_INVALID = "target_invalid"
RESULT_TARGET_RELEASE_GROUP_MISMATCH = "target_release_group_mismatch"
RESULT_TARGET_SAME_AS_CURRENT = "target_same_as_current"
RESULT_TARGET_COLLISION_REQUEST = "target_collision_request"
RESULT_MIRROR_UNCONFIGURED = "mirror_unconfigured"
RESULT_TRANSIENT = "transient"


@dataclass(frozen=True)
class ReplaceResult:
    """Outcome of a single ``replace_request_mbid`` call.

    ``outcome`` is one of the ``RESULT_*`` constants. Other fields are
    surfaced conditionally:

    - ``new_request_id``: set on ``RESULT_REPLACED``.
    - ``current_status``: set on ``RESULT_TARGET_COLLISION_REQUEST`` so
      the UI can render "already in pipeline (status=imported)" or the
      "previously abandoned" warning when the existing row is
      ``replaced``.
    - ``descendant_request_id``: set on ``RESULT_WRONG_STATE`` when the
      source row is itself already ``status='replaced'`` — so the UI
      can deep-link to "the new request is at /pipeline/{id}".
    - ``warnings``: filesystem-cleanup failures that did NOT roll back
      the DB change (R26 non-fatal semantics).
    """

    outcome: str
    request_id: int
    new_request_id: int | None = None
    current_status: str | None = None
    descendant_request_id: int | None = None
    error_message: str | None = None
    warnings: tuple[str, ...] = field(default_factory=tuple)


# Type aliases for the injectable dependencies.
MBLookup = Callable[..., dict[str, Any]]
"""Signature: ``mb_lookup(mbid, *, fresh: bool=False) -> dict``. The
default is ``web.mb.get_release``; tests inject a fake."""

DiscogsLookup = Callable[..., dict[str, Any]]
"""Signature: ``discogs_lookup(release_id: int, *, fresh: bool=False) ->
dict``. The default is ``web.discogs.get_release``; tests inject a fake
that raises the real ``HTTPError``/``URLError``/``DiscogsMirrorNotConfigured``
on failure paths (test-fidelity Rule B)."""

BeetsDBFactory = Callable[[], Any]
"""Zero-arg callable returning a ``BeetsDB`` instance. Default uses
``lib.beets_db.BeetsDB`` against the configured library path."""


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


def _default_beets_db_factory() -> Any:
    """Default beets DB factory — production callers pass an explicit
    factory but tests and CLI scripts use this fallback."""
    from lib.beets_db import BeetsDB
    return BeetsDB()


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

    def replace_request_mbid(
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
           pre-supersede state (artist/title for staging path,
           imported_path for Plex partial scan, old release id, status).
           The fresh re-read closes the race window where the importer
           worker finished between Phase 0 and Phase 1 — stale
           ``old_status`` would skip beets cleanup; stale
           ``old_imported_path`` would mis-route the Plex rescan.
        3. DB transaction: ``supersede_request_mbid`` atomically flips
           the old row's status, clears ``imported_path``, inserts the
           new row, inserts tracks.
        4. Filesystem cleanup (non-fatal warnings collected):
           - beets removal if old was imported
             (``clear_pipeline_state=False`` so characteristic fields
             stay frozen on the audit row)
           - wrong-matches group delete
           - staging folder rmtree (skipped when old was downloading)
        5. Post-cleanup (advisory lock RELEASED first): regenerate
           search plan for the new request, trigger Meelo / Plex /
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

        source_mbid = source.get("mb_release_id")

        # Pathway-aware target gate (replaces the old step-0a UUID gate).
        # The target must be a valid release id in the SAME identity space
        # as the source: UUID target ⇒ MB source, numeric target ⇒ Discogs
        # source. A cross-pathway target (or an unparseable id) is
        # RESULT_TARGET_INVALID — cross-pathway Replace is out of scope
        # (R4 / AE2). ``detect_release_source`` is the single authority for
        # the pathway (KTD-2); the branch below dispatches on the source's
        # own shape, so MB×MB flows through the original path untouched.
        source_source = detect_release_source(source_mbid)
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
            # A source row without an MBID (Discogs-only) cannot be
            # RG-resolved — same TARGET_INVALID outcome the lookup
            # exception path produced before the typed narrowing.
            if not isinstance(source_mbid, str) or not source_mbid:
                return ReplaceResult(
                    outcome=RESULT_TARGET_INVALID,
                    request_id=request_id,
                    error_message=(
                        f"source MBID {source_mbid!r} could not be "
                        "resolved: source request has no MB release id"
                    ),
                )
            # Lazy-backfill: resolve the source MBID's RG fresh.
            try:
                src_data = self.mb_lookup(source_mbid, fresh=True)
            except _TRANSIENT_LOOKUP_EXCEPTIONS as exc:
                # Network blip / timeout / malformed JSON — retryable.
                return ReplaceResult(
                    outcome=RESULT_TRANSIENT,
                    request_id=request_id,
                    error_message=f"MB lookup failed (transient): {exc}",
                )
            except Exception as exc:  # noqa: BLE001
                return ReplaceResult(
                    outcome=RESULT_TARGET_INVALID,
                    request_id=request_id,
                    error_message=(
                        f"source MBID {source_mbid} could not be "
                        f"resolved: {exc}"
                    ),
                )
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
        try:
            target_data = self.mb_lookup(
                target_mb_release_id, fresh=True
            )
        except _TRANSIENT_LOOKUP_EXCEPTIONS as exc:
            # Network blip / timeout / malformed JSON — retryable.
            return ReplaceResult(
                outcome=RESULT_TRANSIENT,
                request_id=request_id,
                error_message=f"MB lookup failed (transient): {exc}",
            )
        except Exception as exc:  # noqa: BLE001
            return ReplaceResult(
                outcome=RESULT_TARGET_INVALID,
                request_id=request_id,
                error_message=(
                    f"target MBID {target_mb_release_id} could not be "
                    f"resolved: {exc}"
                ),
            )

        if not target_data:
            return ReplaceResult(
                outcome=RESULT_TARGET_INVALID,
                request_id=request_id,
                error_message=(
                    f"target MBID {target_mb_release_id} returned empty "
                    "payload from MB mirror"
                ),
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
            source_mbid=source_mbid,
            canonical_mbid=canonical_mbid,
            target_rg=target_rg,
            target_data=target_data,
            new_discogs_release_id=None,
        )

    def _replace_discogs_target(
        self,
        request_id: int,
        source: dict[str, Any],
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
        from web.discogs import DiscogsMirrorNotConfigured

        normalized_target = normalize_release_id(target_mb_release_id)
        target_id_num = int(normalized_target)

        # Resolve the source master (guardrail before the target IO).
        source_master = source.get("mb_release_group_id")
        if not source_master:
            try:
                src_data = self.discogs_lookup(
                    int(normalize_release_id(source_mbid)), fresh=True,
                )
            except DiscogsMirrorNotConfigured as exc:
                return ReplaceResult(
                    outcome=RESULT_MIRROR_UNCONFIGURED,
                    request_id=request_id,
                    error_message=f"Discogs mirror not configured: {exc}",
                )
            except _TRANSIENT_LOOKUP_EXCEPTIONS as exc:
                return ReplaceResult(
                    outcome=RESULT_TRANSIENT,
                    request_id=request_id,
                    error_message=(
                        f"Discogs lookup failed (transient): {exc}"
                    ),
                )
            except Exception as exc:  # noqa: BLE001
                return ReplaceResult(
                    outcome=RESULT_TARGET_INVALID,
                    request_id=request_id,
                    error_message=(
                        f"source Discogs id {source_mbid} could not be "
                        f"resolved: {exc}"
                    ),
                )
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
        try:
            target_data = self.discogs_lookup(target_id_num, fresh=True)
        except DiscogsMirrorNotConfigured as exc:
            return ReplaceResult(
                outcome=RESULT_MIRROR_UNCONFIGURED,
                request_id=request_id,
                error_message=f"Discogs mirror not configured: {exc}",
            )
        except _TRANSIENT_LOOKUP_EXCEPTIONS as exc:
            return ReplaceResult(
                outcome=RESULT_TRANSIENT,
                request_id=request_id,
                error_message=f"Discogs lookup failed (transient): {exc}",
            )
        except Exception as exc:  # noqa: BLE001
            return ReplaceResult(
                outcome=RESULT_TARGET_INVALID,
                request_id=request_id,
                error_message=(
                    f"target Discogs id {target_mb_release_id} could not "
                    f"be resolved: {exc}"
                ),
            )

        if not target_data:
            return ReplaceResult(
                outcome=RESULT_TARGET_INVALID,
                request_id=request_id,
                error_message=(
                    f"target Discogs id {target_mb_release_id} returned "
                    "empty payload from the mirror"
                ),
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
            source_mbid=source_mbid,
            canonical_mbid=canonical_id,
            target_rg=target_master,
            target_data=target_data,
            new_discogs_release_id=canonical_id,
        )

    def _finalize_replace(
        self,
        request_id: int,
        *,
        source_mbid: str | None,
        canonical_mbid: str,
        target_rg: str,
        target_data: dict[str, Any],
        new_discogs_release_id: str | None,
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
        # worker cannot finish (and flip status / set imported_path)
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
            old_imported_path = source_locked.get("imported_path")
            old_release_id = source_locked.get("mb_release_id") or source_mbid
            old_status = source_locked.get("status")

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

            # Phase 4 — filesystem cleanup (non-fatal).
            if old_status == "imported" and old_release_id:
                try:
                    beets_db = self.beets_db_factory()
                    result = remove_and_reset_release(
                        beets_db=beets_db,
                        pipeline_db=self.db,
                        release_id=old_release_id,
                        request_id=request_id,
                        clear_pipeline_state=False,
                    )
                    for failure in result.selector_failures:
                        warnings.append(
                            f"beets selector failed "
                            f"{getattr(failure, 'selector', '?')}: "
                            f"{getattr(failure, 'reason', '?')}"
                        )
                except Exception as exc:  # noqa: BLE001
                    warnings.append(
                        f"beets removal raised "
                        f"{type(exc).__name__}: {exc}"
                    )

            try:
                wm_summary = delete_wrong_match_group(self.db, request_id)
                if wm_summary.errors:
                    warnings.append(
                        f"wrong-matches cleanup reported "
                        f"{wm_summary.errors} errors "
                        f"({wm_summary.remaining} remaining)"
                    )
            except Exception as exc:  # noqa: BLE001
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
                        except Exception as exc:  # noqa: BLE001
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
        except Exception as exc:  # noqa: BLE001
            warnings.append(
                f"search-plan generation failed for new request "
                f"{new_request_id}: {type(exc).__name__}: {exc}"
            )

        try:
            trigger_meelo_scan(self.config)
        except Exception as exc:  # noqa: BLE001
            warnings.append(
                f"meelo rescan failed: {type(exc).__name__}: {exc}"
            )
        try:
            trigger_plex_scan(
                self.config, imported_path=old_imported_path
            )
        except Exception as exc:  # noqa: BLE001
            warnings.append(
                f"plex rescan failed: {type(exc).__name__}: {exc}"
            )
        try:
            trigger_jellyfin_scan(self.config)
        except Exception as exc:  # noqa: BLE001
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
