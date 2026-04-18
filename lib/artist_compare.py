"""Fuzzy merge of MB + Discogs artist discographies.

Pure functions — no I/O. The web layer fetches both source's discographies
and hands them to merge_discographies(), which buckets them into:

  - 'both'        : (mb_rg, discogs_master) pairs that appear to be the same
                    logical album (matched on normalized title + year ±tol)
  - 'mb_only'     : MB release groups with no Discogs counterpart
  - 'discogs_only': Discogs masters with no MB counterpart

Heuristic is deliberately conservative — when in doubt, rows stay separate
rather than risk a false merge that would hide data from the user.
"""

from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass

# Title normalization: lowercase + strip non-alphanumeric. Catches "Text_Bomb"
# vs "Text Bomb" and "OK Computer" vs "Ok. Computer".
_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")


def normalize_title(title: str) -> str:
    """Lowercase + strip non-alphanumeric for fuzzy comparison."""
    return _NON_ALNUM_RE.sub("", (title or "").lower())


def extract_year(date_str: str) -> int | None:
    """Pull the year from a Discogs/MB date string ('1997', '1997-06-16')."""
    if not date_str or len(date_str) < 4:
        return None
    try:
        return int(date_str[:4])
    except ValueError:
        return None


@dataclass
class CompareBuckets:
    both: list[dict]
    mb_only: list[dict]
    discogs_only: list[dict]


def annotate_in_library(
    mb_groups: list[dict],
    discogs_groups: list[dict],
    library_albums: list[dict],
) -> None:
    """Mutate each row in mb_groups + discogs_groups to add `in_library: bool`.

    A row is considered in-library if any beets album from this artist
    matches it via:
      - mb_releasegroupid (MB rows)
      - mb_albumid (Discogs rows — beets stores numeric Discogs IDs there)
      - normalized title (catches Discogs imports tagged with MB IDs and
        cross-source matches the explicit ID lookup misses)

    Mutates in place. Intended to be called once per request after the
    discography + library albums are fetched, so the front end gets the
    same in-library badge at the row level that the expanded pressings
    view shows per pressing.
    """
    lib_rgids: set[str] = set()
    lib_mbids: set[str] = set()
    lib_titles: set[str] = set()
    for a in library_albums:
        rgid = a.get("mb_releasegroupid")
        if rgid:
            lib_rgids.add(str(rgid))
        mbid = a.get("mb_albumid")
        if mbid:
            lib_mbids.add(str(mbid))
        title = a.get("album", "")
        if title:
            lib_titles.add(normalize_title(title))

    for rg in mb_groups:
        rid = str(rg.get("id", ""))
        title_norm = normalize_title(rg.get("title", ""))
        rg["in_library"] = (
            (rid and rid in lib_rgids)
            or (title_norm and title_norm in lib_titles)
        )

    for master in discogs_groups:
        mid = str(master.get("id", ""))
        title_norm = normalize_title(master.get("title", ""))
        master["in_library"] = (
            (mid and mid in lib_mbids)
            or (title_norm and title_norm in lib_titles)
        )


def merge_discographies(
    mb_groups: list[dict],
    discogs_groups: list[dict],
    year_tolerance: int = 2,
) -> CompareBuckets:
    """Bucket MB + Discogs release groups by fuzzy title+year match.

    Match rule, in order:
      1. Normalized title equality is required.
      2. If both years are known: |mb_year - discogs_year| <= year_tolerance.
      3. If both years are unknown: title alone is enough.
      4. If exactly one year is known: skip — too ambiguous to merge safely.

    Each Discogs entry can match at most one MB entry.
    """
    by_norm: dict[str, list[int]] = defaultdict(list)
    for i, d in enumerate(discogs_groups):
        norm = normalize_title(d.get("title", ""))
        if norm:
            by_norm[norm].append(i)

    matched: set[int] = set()
    both: list[dict] = []
    mb_only: list[dict] = []

    for m in mb_groups:
        norm = normalize_title(m.get("title", ""))
        m_year = extract_year(m.get("first_release_date", ""))
        match_idx: int | None = None
        for di in by_norm.get(norm, []):
            if di in matched:
                continue
            d_year = extract_year(discogs_groups[di].get("first_release_date", ""))
            if m_year is not None and d_year is not None:
                if abs(m_year - d_year) <= year_tolerance:
                    match_idx = di
                    break
            elif m_year is None and d_year is None:
                match_idx = di
                break
            # Mixed (one known, one unknown) — skip; ambiguous.
        if match_idx is not None:
            matched.add(match_idx)
            both.append({"mb": m, "discogs": discogs_groups[match_idx]})
        else:
            mb_only.append(m)

    discogs_only = [d for i, d in enumerate(discogs_groups) if i not in matched]
    return CompareBuckets(both=both, mb_only=mb_only, discogs_only=discogs_only)
