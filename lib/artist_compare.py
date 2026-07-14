"""Conservative MB/Discogs artist-catalogue comparison.

The adapters feed this module one shared semantic row shape. MusicBrainz
release groups, Discogs masters, and masterless Discogs releases all describe
the artist's semantic catalogue. A masterless row remains
``identity_kind='release'`` even when it associates with an MB work: display
association never rewrites pressing identity or authorizes substitution.
"""

from __future__ import annotations

import re
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass

from lib.artist_catalogue import ArtistCataloguePair, ArtistCatalogueRow


_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")
_STRUCTURAL_TYPES = frozenset({"Album", "EP", "Single"})


def normalize_title(title: str) -> str:
    """Lowercase + strip non-alphanumeric for conservative comparison."""
    return _NON_ALNUM_RE.sub("", (title or "").lower())


def extract_year(date_str: str) -> int | None:
    """Pull the year from a Discogs/MB date string."""
    if not date_str or len(date_str) < 4:
        return None
    try:
        return int(date_str[:4])
    except ValueError:
        return None


@dataclass
class CompareBuckets:
    both: list[ArtistCataloguePair]
    mb_unpaired: list[ArtistCatalogueRow]
    discogs_unpaired: list[ArtistCatalogueRow]
    discogs_ungrouped_releases: list[ArtistCatalogueRow]


def annotate_in_library(
    mb_groups: list[ArtistCatalogueRow],
    discogs_groups: list[ArtistCatalogueRow],
    library_albums: list[dict],
    rank_fn: Callable[[str, int], str] | None = None,
) -> None:
    """Add identifier-backed ownership and quality overlays in place.

    MB work rows match an exact ``mb_releasegroupid``. Discogs release rows
    match the exact numeric release id stored by the beets Discogs plugin in
    ``mb_albumid``. Discogs masters never inherit a child release's ownership,
    and titles never act as identity.
    """
    lib_by_rgid: dict[str, dict] = {}
    lib_by_release_id: dict[str, dict] = {}
    for album in library_albums:
        rgid = album.get("mb_releasegroupid")
        if rgid and str(rgid) not in lib_by_rgid:
            lib_by_rgid[str(rgid)] = album
        release_id = album.get("mb_albumid")
        if release_id and str(release_id) not in lib_by_release_id:
            lib_by_release_id[str(release_id)] = album

    def attach(row: ArtistCatalogueRow, match: dict) -> None:
        row.in_library = True
        fmt = match.get("formats") or ""
        min_br_bps = match.get("min_bitrate") or 0
        avg_br_bps = match.get("avg_bitrate") or 0
        min_kbps = (min_br_bps // 1000) if min_br_bps else 0
        avg_kbps = (avg_br_bps // 1000) if avg_br_bps else 0
        row.library_format = fmt
        row.library_min_bitrate = min_kbps
        row.library_avg_bitrate = avg_kbps
        if rank_fn:
            row.library_rank = rank_fn(fmt, avg_kbps)

    for row in mb_groups:
        match = lib_by_rgid.get(row.id)
        if match:
            attach(row, match)
        else:
            row.in_library = False

    for row in discogs_groups:
        match = None
        if row.identity_kind == "release":
            match = lib_by_release_id.get(row.id)
        if match:
            attach(row, match)
        else:
            row.in_library = False


def _structural_types(row: ArtistCatalogueRow) -> frozenset[str]:
    """Read positive structural membership from the common row contract."""
    return frozenset(
        value for value in row.primary_types if value in _STRUCTURAL_TYPES
    )


def _provenance_compatible(
    mb_row: ArtistCatalogueRow,
    discogs_row: ArtistCatalogueRow,
) -> bool:
    """Reject positive conflicts without treating unknown as a conflict."""
    mb = frozenset(mb_row.provenance)
    discogs = frozenset(discogs_row.provenance)
    if not mb or not discogs:
        return True
    return bool(mb & discogs)


def merge_discographies(
    mb_groups: list[ArtistCatalogueRow],
    discogs_groups: list[ArtistCatalogueRow],
) -> CompareBuckets:
    """Associate semantic catalogue rows and conserve exact identities.

    Work pairing requires normalized-title equality, matching appearance
    provenance, no positive provenance conflict, no known
    structural-type conflict, and a conservative date rule. Exact years may
    pair when one source has unknown structural type; adjacent years require
    positive overlapping structural evidence. Unknown provenance is not
    negative evidence; when both sides are known their evidence sets must
    overlap. Each source identity remains present exactly once across the
    returned buckets, and a paired Discogs release remains a release.
    """
    if any(row.identity_kind != "work" for row in mb_groups):
        raise ValueError("MusicBrainz artist rows must be work identities")

    if any(
        row.identity_kind not in {"work", "release"}
        for row in discogs_groups
    ):
        raise ValueError("unknown Discogs artist identity kind")

    by_norm: dict[str, list[int]] = defaultdict(list)
    for index, row in enumerate(discogs_groups):
        norm = normalize_title(row.title)
        if norm:
            by_norm[norm].append(index)

    candidate_edges: list[tuple[tuple[int, int], int, int]] = []
    for mb_index, mb_row in enumerate(mb_groups):
        norm = normalize_title(mb_row.title)
        mb_year = extract_year(mb_row.first_release_date)
        mb_types = _structural_types(mb_row)
        mb_appearance = mb_row.is_appearance

        for discogs_index in by_norm.get(norm, []):
            discogs_row = discogs_groups[discogs_index]
            if mb_appearance != discogs_row.is_appearance:
                continue
            if not _provenance_compatible(mb_row, discogs_row):
                continue

            discogs_year = extract_year(
                discogs_row.first_release_date
            )
            discogs_types = _structural_types(discogs_row)
            type_overlap = bool(mb_types & discogs_types)
            if mb_types and discogs_types and not type_overlap:
                continue

            if mb_year is None and discogs_year is None:
                year_score = 1
            elif (
                mb_year is not None
                and discogs_year is not None
                and mb_year == discogs_year
            ):
                year_score = 3
            elif (
                mb_year is not None
                and discogs_year is not None
                and abs(mb_year - discogs_year) == 1
                and mb_types
                and discogs_types
                and type_overlap
            ):
                year_score = 2
            else:
                continue

            candidate_edges.append(
                ((year_score, int(type_overlap)), mb_index, discogs_index)
            )

    candidate_edges.sort(
        key=lambda edge: (-edge[0][0], -edge[0][1], edge[1], edge[2])
    )
    matched_mb: set[int] = set()
    matched_discogs: set[int] = set()
    mb_to_discogs: dict[int, int] = {}
    for _score, mb_index, discogs_index in candidate_edges:
        if mb_index in matched_mb or discogs_index in matched_discogs:
            continue
        matched_mb.add(mb_index)
        matched_discogs.add(discogs_index)
        mb_to_discogs[mb_index] = discogs_index

    both: list[ArtistCataloguePair] = []
    mb_unpaired: list[ArtistCatalogueRow] = []
    for mb_index, mb_row in enumerate(mb_groups):
        discogs_index = mb_to_discogs.get(mb_index)
        if discogs_index is None:
            mb_unpaired.append(mb_row)
        else:
            both.append(ArtistCataloguePair(
                mb=mb_row,
                discogs=discogs_groups[discogs_index],
            ))

    unmatched_discogs = [
        row
        for index, row in enumerate(discogs_groups)
        if index not in matched_discogs
    ]
    return CompareBuckets(
        both=both,
        mb_unpaired=mb_unpaired,
        discogs_unpaired=[
            row for row in unmatched_discogs if row.identity_kind == "work"
        ],
        discogs_ungrouped_releases=[
            row for row in unmatched_discogs if row.identity_kind == "release"
        ],
    )
