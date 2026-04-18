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
    rank_fn=None,
) -> None:
    """Mutate each row in mb_groups + discogs_groups to add in-library flags.

    Sets per-row:
      - in_library: bool
      - library_format: str (e.g. "MP3", "FLAC") — only when matched
      - library_min_bitrate: int (kbps) — only when matched
      - library_rank: str — only when matched and rank_fn provided
        (lowercase rank name from lib.quality.QualityRank, e.g.
        "transparent", "lossless", "poor")

    Match strategies (any one is enough):
      - mb_releasegroupid (MB rows)
      - mb_albumid (Discogs rows — beets stores numeric Discogs IDs there)
      - normalized title fallback (cross-source)

    rank_fn: optional callable(format_str, bitrate_kbps) -> str. Lets the
    caller plug in the codec-aware quality_rank computation without
    pulling the rank config into this pure module.
    """
    # Map -> matched album dict (not just bool) so we can also set
    # quality fields when a row hits.
    lib_by_rgid: dict[str, dict] = {}
    lib_by_mbid: dict[str, dict] = {}
    lib_by_title: dict[str, dict] = {}
    for a in library_albums:
        rgid = a.get("mb_releasegroupid")
        if rgid and str(rgid) not in lib_by_rgid:
            lib_by_rgid[str(rgid)] = a
        mbid = a.get("mb_albumid")
        if mbid and str(mbid) not in lib_by_mbid:
            lib_by_mbid[str(mbid)] = a
        title = a.get("album", "")
        if title:
            tn = normalize_title(title)
            if tn and tn not in lib_by_title:
                lib_by_title[tn] = a

    def _attach(row: dict, match: dict) -> None:
        row["in_library"] = True
        fmt = match.get("formats") or ""
        # min_bitrate from beets is in bps; convert to kbps for display
        # consistency.
        br_bps = match.get("min_bitrate") or 0
        kbps = (br_bps // 1000) if br_bps else 0
        row["library_format"] = fmt
        row["library_min_bitrate"] = kbps
        if rank_fn:
            row["library_rank"] = rank_fn(fmt, kbps)

    for rg in mb_groups:
        rid = str(rg.get("id", ""))
        title_norm = normalize_title(rg.get("title", ""))
        match = lib_by_rgid.get(rid) or (lib_by_title.get(title_norm) if title_norm else None)
        if match:
            _attach(rg, match)
        else:
            rg["in_library"] = False

    for master in discogs_groups:
        mid = str(master.get("id", ""))
        title_norm = normalize_title(master.get("title", ""))
        match = lib_by_mbid.get(mid) or (lib_by_title.get(title_norm) if title_norm else None)
        if match:
            _attach(master, match)
        else:
            master["in_library"] = False


def _dedupe_within_source(rows: list[dict], year_tolerance: int) -> list[dict]:
    """Collapse same-normalized-title + year-within-tolerance duplicates.

    Within a single source, multiple entries with effectively the same
    title (e.g. "Twist And Shout" and "Twist and Shout" — different
    Discogs masters from data-entry inconsistency) read as duplicates
    once normalised. Keep the first (input order) as canonical, drop
    the rest. Matches the merge function's same-title+year-tolerance
    rule so a row that survives dedup also survives cross-source merge.
    """
    seen: list[tuple[str, int | None]] = []
    result: list[dict] = []
    for r in rows:
        norm = normalize_title(r.get("title", ""))
        if not norm:
            result.append(r)
            continue
        year = extract_year(r.get("first_release_date", ""))
        is_dup = False
        for s_norm, s_year in seen:
            if s_norm != norm:
                continue
            if year is None or s_year is None:
                is_dup = True
                break
            if abs(year - s_year) <= year_tolerance:
                is_dup = True
                break
        if not is_dup:
            seen.append((norm, year))
            result.append(r)
    return result


def merge_discographies(
    mb_groups: list[dict],
    discogs_groups: list[dict],
    year_tolerance: int = 2,
) -> CompareBuckets:
    """Bucket MB + Discogs release groups by fuzzy title+year match.

    Pre-pass: collapse within-source duplicates (e.g. two Discogs
    masters that only differ in title casing) so the cross-source
    merge sees one logical row per album per source.

    Match rule, in order:
      1. Normalized title equality is required.
      2. If both years are known: |mb_year - discogs_year| <= year_tolerance.
      3. If both years are unknown: title alone is enough.
      4. If exactly one year is known: skip — too ambiguous to merge safely.

    Each Discogs entry can match at most one MB entry.
    """
    mb_groups = _dedupe_within_source(mb_groups, year_tolerance)
    discogs_groups = _dedupe_within_source(discogs_groups, year_tolerance)

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
