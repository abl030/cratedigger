"""Crash-safe publication for direct Add and new-row Upgrade requests.

The add adapters fetch and normalise source-specific metadata first.  This
service then serialises the exact release, writes a deliberately non-runnable
row, and publishes it only after all creation-owned persistence has completed.
It is intentionally not a transaction spanning mirror I/O: the durable
``initializing`` state is the recovery point.
"""

from __future__ import annotations

from contextlib import AbstractContextManager
from dataclasses import dataclass, field
from typing import Callable, Literal, Protocol

from lib import transitions
from lib.config import CratediggerConfig
from lib.field_resolver_service import (
    apply_resolve_all_result,
    resolve_all,
)
from lib.pipeline_db import (
    ADVISORY_LOCK_NAMESPACE_RELEASE,
    release_id_to_lock_key,
)
from lib.pipeline_db.rows import AlbumRequestRow
from lib.search_plan_service import SearchPlanDB, SearchPlanService, ServiceResult


CreationOutcome = Literal[
    "created", "resumed", "exists", "busy", "initialization_failed",
]


def _empty_final_fields() -> dict[str, object]:
    return {}


@dataclass(frozen=True)
class RequestCreationInput:
    """Normalised source payload required to create or resume one request.

    ``release_id`` is the canonical exact identity used for both the RELEASE
    lock and the in-lock database lookup.  Discogs callers deliberately pass
    their numeric ID in both identity columns; no MB/Discogs adapter is hidden
    here.
    """

    release_id: str
    artist_name: str
    album_title: str
    source: str
    tracks: list[dict[str, object]]
    mb_release_id: str | None = None
    mb_release_group_id: str | None = None
    mb_artist_id: str | None = None
    discogs_release_id: str | None = None
    year: int | None = None
    country: str | None = None
    release_group_year: int | None = None
    mb_release_payload: dict[str, object] | None = None
    discogs_release_payload: dict[str, object] | None = None
    # New-row Upgrade carries its policy into the same CAS that publishes.
    final_fields: dict[str, object] = field(default_factory=_empty_final_fields)


@dataclass(frozen=True)
class RequestCreationResult:
    outcome: CreationOutcome
    request_id: int | None = None
    detail: str | None = None


class RequestCreationDB(SearchPlanDB, transitions.TransitionsDB, Protocol):
    def advisory_lock(
        self, namespace: int, key: int,
    ) -> AbstractContextManager[bool]: ...

    def get_request_by_release_id(
        self, release_id: object | None,
    ) -> AlbumRequestRow | None: ...

    def add_request(
        self,
        *,
        artist_name: str,
        album_title: str,
        source: str,
        mb_release_id: str | None = None,
        mb_release_group_id: str | None = None,
        mb_artist_id: str | None = None,
        discogs_release_id: str | None = None,
        year: int | None = None,
        country: str | None = None,
        format: str | None = None,
        source_path: str | None = None,
        reasoning: str | None = None,
        status: str = "wanted",
        release_group_year: int | None = None,
        is_va_compilation: bool = False,
    ) -> int: ...

    def set_tracks(self, request_id: int, tracks: list[dict[str, object]]) -> None: ...

    def update_request_fields(
        self,
        request_id: int,
        *,
        expected_status: str | None = None,
        **fields: object,
    ) -> bool: ...

    def update_track_artists(
        self, request_id: int, track_artists: list[str | None], *,
        expected_status: str | None = None,
    ) -> bool: ...

    def record_field_resolution(
        self, request_id: int, field_name: str, status: str,
        reason_code: str | None,
    ) -> bool: ...

class NewRequestPlanService(Protocol):
    def generate_for_new_request(
        self, request_id: int, *, artist_name: str, album_title: str,
        year: object, tracks: list[dict[str, object]], source: str = "request",
        prepend_artist: bool | None = None, release_group_year: object = None,
        is_va_compilation: bool = False, catalog_number: object = None,
    ) -> ServiceResult: ...


PlanServiceFactory = Callable[
    [RequestCreationDB, CratediggerConfig], NewRequestPlanService,
]


class RequestCreationService:
    """Create or resume an exact request without ever publishing it early."""

    def __init__(
        self,
        db: RequestCreationDB,
        config: CratediggerConfig,
        *,
        plan_service_factory: PlanServiceFactory = SearchPlanService,
    ) -> None:
        self.db = db
        self.config = config
        self.plan_service_factory = plan_service_factory

    def create_or_resume(
        self, creation: RequestCreationInput,
    ) -> RequestCreationResult:
        """Perform every durable boundary under RELEASE then PLAN lock order.

        Any exception after insertion deliberately returns the request id and
        leaves it ``initializing``. Reissuing the same operation performs the
        idempotent writes again and is the sole recovery mechanism.
        """
        with self.db.advisory_lock(
            ADVISORY_LOCK_NAMESPACE_RELEASE,
            release_id_to_lock_key(creation.release_id),
        ) as acquired:
            if not acquired:
                return RequestCreationResult("busy", detail="release lock held")
            existing = self.db.get_request_by_release_id(creation.release_id)
            resumed = existing is not None and existing["status"] == "initializing"
            if existing is not None and not resumed:
                return RequestCreationResult("exists", request_id=existing["id"])
            request_id = (
                existing["id"] if existing is not None else self.db.add_request(
                    artist_name=creation.artist_name,
                    album_title=creation.album_title,
                    source=creation.source,
                    mb_release_id=creation.mb_release_id,
                    mb_release_group_id=creation.mb_release_group_id,
                    mb_artist_id=creation.mb_artist_id,
                    discogs_release_id=creation.discogs_release_id,
                    year=creation.year,
                    country=creation.country,
                    release_group_year=creation.release_group_year,
                    status="initializing",
                )
            )
            try:
                self.db.set_tracks(request_id, creation.tracks)
                resolved = resolve_all(
                    {
                        "id": request_id,
                        # Modern Discogs rows dual-write the numeric ID into
                        # mb_release_id for DB identity compatibility. That
                        # is not MusicBrainz resolver input: keep source
                        # dispatch unambiguous while preserving the stored
                        # dual-write above.
                        "mb_release_id": (
                            None if creation.discogs_release_id is not None
                            else creation.mb_release_id
                        ),
                        "discogs_release_id": creation.discogs_release_id,
                        "mb_release_group_id": creation.mb_release_group_id,
                        "mb_artist_id": creation.mb_artist_id,
                    },
                    self.db,
                    mb_release_payload=creation.mb_release_payload,
                    discogs_release_payload=creation.discogs_release_payload,
                    strict_persistence=True,
                )
                if not apply_resolve_all_result(
                    self.db,
                    request_id,
                    resolved,
                    expected_status="initializing",
                    existing_mb_release_group_id=creation.mb_release_group_id,
                    strict=True,
                ):
                    return self._failed(request_id, "request changed during resolution")
                plan = self.plan_service_factory(self.db, self.config).generate_for_new_request(
                    request_id,
                    artist_name=creation.artist_name,
                    album_title=creation.album_title,
                    year=creation.year,
                    tracks=self.db.get_tracks(request_id),
                    source=creation.source,
                    release_group_year=(
                        resolved.release_group_year
                        if resolved.release_group_year is not None
                        else creation.release_group_year
                    ),
                    is_va_compilation=resolved.is_va_compilation,
                    catalog_number=resolved.catalog_number,
                )
                # A no-id result means no durable outcome was reported. Do
                # not infer success from a pre-existing active plan: that
                # plan may be unrelated/stale and the current service call
                # has not proved its own creation boundary persisted.
                if plan.plan_id is None:
                    return self._failed(
                        request_id, plan.error_message or "plan not persisted",
                    )
                # Publication is deliberately not an ordinary lifecycle
                # transition. Only this service may CAS initializing → wanted.
                publication = transitions.publish_initialized_request(
                    self.db, request_id, fields=creation.final_fields,
                )
                if isinstance(publication, transitions.TransitionConflict):
                    return self._failed(request_id, "publication CAS lost")
            except Exception:  # noqa: BLE001
                # Resolver/database exceptions may contain upstream URLs or
                # implementation detail. The retained request id is the
                # recovery handle; keep the adapter diagnostic safe.
                return self._failed(request_id, "initialization did not complete")
            return RequestCreationResult(
                "resumed" if resumed else "created", request_id=request_id,
            )

    @staticmethod
    def _failed(request_id: int, detail: str) -> RequestCreationResult:
        return RequestCreationResult(
            "initialization_failed", request_id=request_id, detail=detail,
        )
