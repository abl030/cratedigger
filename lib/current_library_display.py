"""Typed current-library authority for request detail displays."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Literal, Protocol, TypeAlias

import msgspec

from lib.beets_db import (
    CurrentBeetsAmbiguous,
    CurrentBeetsAmbiguityReason,
    CurrentBeetsMissing,
    CurrentBeetsResolution,
    CurrentBeetsUnique,
)
from lib.release_identity import ReleaseIdentity, normalize_release_id


CurrentLibraryUnavailableReason: TypeAlias = Literal[
    "beets_unavailable",
    "missing_request_identity",
    "invalid_request_identity",
    "conflicting_request_identity",
]


class CurrentLibraryReader(Protocol):
    """Small resolver surface shared by the real and fake Beets stores."""

    def resolve_current_release(
        self,
        identity: ReleaseIdentity,
    ) -> CurrentBeetsResolution: ...


class CurrentLibraryUnavailable(msgspec.Struct, frozen=True):
    """Request state cannot select one exact Beets identity safely."""

    reason: CurrentLibraryUnavailableReason


CurrentLibraryResolution: TypeAlias = (
    CurrentBeetsUnique
    | CurrentBeetsMissing
    | CurrentBeetsAmbiguous
    | CurrentLibraryUnavailable
)


class CurrentLibraryUniqueDisplay(
    msgspec.Struct,
    frozen=True,
    tag="unique",
    tag_field="state",
):
    release_source: Literal["musicbrainz", "discogs"]
    release_id: str
    album_id: int
    path: str


class CurrentLibraryMissingDisplay(
    msgspec.Struct,
    frozen=True,
    tag="missing",
    tag_field="state",
):
    release_source: Literal["musicbrainz", "discogs"]
    release_id: str


class CurrentLibraryAmbiguousDisplay(
    msgspec.Struct,
    frozen=True,
    tag="ambiguous",
    tag_field="state",
):
    release_source: Literal["musicbrainz", "discogs"]
    release_id: str
    reason: CurrentBeetsAmbiguityReason
    album_ids: tuple[int, ...]


class CurrentLibraryUnavailableDisplay(
    msgspec.Struct,
    frozen=True,
    tag="unavailable",
    tag_field="state",
):
    reason: CurrentLibraryUnavailableReason
    manual_review: bool = True


CurrentLibraryDisplay: TypeAlias = (
    CurrentLibraryUniqueDisplay
    | CurrentLibraryMissingDisplay
    | CurrentLibraryAmbiguousDisplay
    | CurrentLibraryUnavailableDisplay
)


def _strict_request_identity(
    row: Mapping[str, object],
) -> ReleaseIdentity | CurrentLibraryUnavailable:
    """Use the shared strict authority and diagnose only its rejection."""

    primary = row.get("mb_release_id")
    discogs = row.get("discogs_release_id")
    identity = ReleaseIdentity.from_strict_fields(primary, discogs)
    if identity is not None:
        return identity

    populated: list[str] = []
    for value in (primary, discogs):
        normalized = normalize_release_id(value)
        if not normalized:
            continue
        populated.append(normalized)
        if ReleaseIdentity.from_id(normalized) is None:
            return CurrentLibraryUnavailable("invalid_request_identity")
    if not populated:
        return CurrentLibraryUnavailable("missing_request_identity")
    return CurrentLibraryUnavailable("conflicting_request_identity")


def resolve_request_current_library(
    row: Mapping[str, object],
    beets: CurrentLibraryReader | None,
) -> CurrentLibraryResolution:
    """Resolve one request from fresh exact Beets state, or fail closed."""

    identity = _strict_request_identity(row)
    if isinstance(identity, CurrentLibraryUnavailable):
        return identity
    if beets is None:
        return CurrentLibraryUnavailable("beets_unavailable")
    return beets.resolve_current_release(identity)


def current_library_display(
    resolution: CurrentLibraryResolution,
) -> CurrentLibraryDisplay:
    """Project internal resolver types onto the stable CLI/API wire union."""

    if isinstance(resolution, CurrentBeetsUnique):
        return CurrentLibraryUniqueDisplay(
            release_source=resolution.identity.source,
            release_id=resolution.identity.release_id,
            album_id=resolution.album_id,
            path=resolution.album_path,
        )
    if isinstance(resolution, CurrentBeetsMissing):
        return CurrentLibraryMissingDisplay(
            release_source=resolution.identity.source,
            release_id=resolution.identity.release_id,
        )
    if isinstance(resolution, CurrentBeetsAmbiguous):
        return CurrentLibraryAmbiguousDisplay(
            release_source=resolution.identity.source,
            release_id=resolution.identity.release_id,
            reason=resolution.reason,
            album_ids=resolution.album_ids,
        )
    return CurrentLibraryUnavailableDisplay(reason=resolution.reason)
