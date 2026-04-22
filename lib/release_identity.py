"""Shared exact-release identity helpers for MB UUIDs and Discogs IDs."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Literal

ReleaseSource = Literal["musicbrainz", "discogs", "unknown"]

_UUID_RE = re.compile(
    r"^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$",
    re.IGNORECASE,
)
_NUMERIC_RE = re.compile(r"^\d+$")


def normalize_release_id(raw: object | None) -> str:
    """Canonicalize a release ID string across MB + Discogs shapes."""
    if raw is None:
        return ""
    value = str(raw).strip()
    if not value:
        return ""
    if _UUID_RE.fullmatch(value):
        return value.lower()
    if _NUMERIC_RE.fullmatch(value):
        numeric = int(value)
        # Beets stores "no Discogs id" as 0; never treat that as a real release.
        if numeric <= 0:
            return ""
        return str(numeric)
    return value


def detect_release_source(id_str: object | None) -> ReleaseSource:
    """Detect the release source after normalization."""
    normalized = normalize_release_id(id_str)
    if not normalized:
        return "unknown"
    if _UUID_RE.fullmatch(normalized):
        return "musicbrainz"
    if _NUMERIC_RE.fullmatch(normalized):
        return "discogs"
    return "unknown"


@dataclass(frozen=True)
class ReleaseIdentity:
    """Canonical exact-release identity used across browse/library/delete."""

    source: Literal["musicbrainz", "discogs"]
    release_id: str

    @property
    def key(self) -> tuple[str, str]:
        return (self.source, self.release_id)

    @classmethod
    def from_id(cls, release_id: object | None) -> ReleaseIdentity | None:
        normalized = normalize_release_id(release_id)
        source = detect_release_source(normalized)
        if source == "musicbrainz" or source == "discogs":
            return cls(source=source, release_id=normalized)
        return None

    @classmethod
    def from_fields(
        cls,
        release_id: object | None,
        discogs_release_id: object | None = None,
    ) -> ReleaseIdentity | None:
        """Pick the canonical exact-release identity from a row's fields."""
        primary = cls.from_id(release_id)
        if primary and primary.source == "musicbrainz":
            return primary

        discogs = cls.from_id(discogs_release_id)
        if discogs and discogs.source == "discogs":
            return discogs

        return primary


def frontend_release_id(
    release_id: object | None,
    discogs_release_id: object | None = None,
) -> str | None:
    """Return the single frontend release-id field for a row, if any."""
    identity = ReleaseIdentity.from_fields(release_id, discogs_release_id)
    return identity.release_id if identity else None
