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
    transient                      503 / 5

See ``docs/plans/2026-05-18-001-feat-replace-operator-action-plan.md``
and ``docs/brainstorms/2026-05-18-replace-operator-action-requirements.md``
for the full design.
"""

from __future__ import annotations

import logging
import os
import shutil
from dataclasses import dataclass, field
from typing import Any, Callable, Optional
from urllib.error import URLError

from lib.config import CratediggerConfig
from lib.pipeline_db import (
    ADVISORY_LOCK_NAMESPACE_IMPORT,
    MbidCollisionError,
    SupersedeRaceError,
)
from lib.processing_paths import stage_to_ai_path
from lib.release_cleanup import remove_and_reset_release
from lib.search_plan_service import SearchPlanService
from lib.util import (
    trigger_jellyfin_scan,
    trigger_meelo_scan,
    trigger_plex_scan,
)
from lib.wrong_match_delete_service import delete_wrong_match_group

logger = logging.getLogger(__name__)


# Result outcome constants.
RESULT_REPLACED = "replaced"
RESULT_NOT_FOUND = "not_found"
RESULT_WRONG_STATE = "wrong_state"
RESULT_TARGET_INVALID = "target_invalid"
RESULT_TARGET_RELEASE_GROUP_MISMATCH = "target_release_group_mismatch"
RESULT_TARGET_SAME_AS_CURRENT = "target_same_as_current"
RESULT_TARGET_COLLISION_REQUEST = "target_collision_request"
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
    new_request_id: Optional[int] = None
    current_status: Optional[str] = None
    descendant_request_id: Optional[int] = None
    error_message: Optional[str] = None
    warnings: tuple[str, ...] = field(default_factory=tuple)


# Type aliases for the injectable dependencies.
MBLookup = Callable[..., dict[str, Any]]
"""Signature: ``mb_lookup(mbid, *, fresh: bool=False) -> dict``. The
default is ``web.mb.get_release``; tests inject a fake."""

BeetsDBFactory = Callable[[], Any]
"""Zero-arg callable returning a ``BeetsDB`` instance. Default uses
``lib.beets_db.BeetsDB`` against the configured library path."""


def _default_mb_lookup(mbid: str, *, fresh: bool = False) -> dict[str, Any]:
    """Default MB-mirror lookup. Imported lazily so the service module
    doesn't pull in ``web.mb``'s urllib transport at import time."""
    from web.mb import get_release
    return get_release(mbid, fresh=fresh)


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
        db: Any,
        config: CratediggerConfig,
        slskd: Any = None,
        beets_db_factory: BeetsDBFactory | None = None,
        mb_lookup: MBLookup | None = None,
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
        5. Post-cleanup: regenerate search plan for the new request,
           trigger Meelo / Plex / Jellyfin rescans.
        """
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
        if source_mbid == target_mb_release_id:
            return ReplaceResult(
                outcome=RESULT_TARGET_SAME_AS_CURRENT,
                request_id=request_id,
                error_message=(
                    "target MBID equals the source request's current "
                    "MBID"
                ),
            )

        source_rg = source.get("mb_release_group_id")
        if not source_rg:
            # Lazy-backfill: resolve the source MBID's RG fresh.
            try:
                src_data = self.mb_lookup(source_mbid, fresh=True)
            except URLError as exc:
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
            source_rg = src_data.get("release_group_id") if src_data else None
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
        except URLError as exc:
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
                staging_dir = getattr(
                    self.config, "beets_staging_dir", None
                )
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

            # Phase 5 — search plan + rescans.
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

        return ReplaceResult(
            outcome=RESULT_REPLACED,
            request_id=request_id,
            new_request_id=new_request_id,
            warnings=tuple(warnings),
        )
