"""Canonical typed contract for artist-catalogue responses.

MusicBrainz release groups and Discogs masters are work identities. A
masterless Discogs release is a release identity. Adapters normalize into
this one strict shape before matching, grouping, ownership overlays, or
rendering; consumers must not recover semantics from source-specific scalar
fields.
"""

from __future__ import annotations

from typing import Literal

import msgspec


ArtistCatalogueSource = Literal["mb", "discogs"]
ArtistIdentityKind = Literal["work", "release"]
ArtistStructuralType = Literal["Album", "EP", "Single"]
ArtistProvenance = Literal["ordinary", "promo", "unofficial"]


class ArtistCatalogueRow(msgspec.Struct, omit_defaults=True):
    """Normalized source row plus optional per-request overlay fields."""

    id: str
    title: str
    type: str
    source: ArtistCatalogueSource
    identity_kind: ArtistIdentityKind
    primary_types: list[ArtistStructuralType]
    secondary_types: list[str]
    format_qualifiers: list[str]
    provenance: list[ArtistProvenance]
    first_release_date: str
    artist_credit: str
    primary_artist_id: str
    is_appearance: bool
    discogs_release_id: str | None = None
    in_library: bool | None = None
    library_format: str | None = None
    library_min_bitrate: int | None = None
    library_avg_bitrate: int | None = None
    library_rank: str | None = None
    pipeline_status: str | None = None
    pipeline_id: int | None = None


class ArtistCataloguePair(msgspec.Struct):
    """One conservative cross-source work pairing."""

    mb: ArtistCatalogueRow
    discogs: ArtistCatalogueRow


class ArtistCompareSkeleton(msgspec.Struct):
    """Cache-safe, overlay-free cross-source comparison response."""

    both: list[ArtistCataloguePair]
    mb_unpaired: list[ArtistCatalogueRow]
    discogs_unpaired: list[ArtistCatalogueRow]
    discogs_ungrouped_releases: list[ArtistCatalogueRow]
