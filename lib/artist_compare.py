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

    Match strategies, tried in order (first hit wins):
      1. mb_releasegroupid (MB rows ↔ library rows tagged with MB RGID)
      2. mb_albumid (Discogs rows ↔ library rows where beets stored the
         numeric Discogs ID in mb_albumid — Discogs plugin convention)
      3. normalized title fallback — the CROSS-SOURCE bridge

    Why the title fallback is deliberate (not a #123-style smell):
      MB and Discogs live in two ID namespaces that do not share primary
      keys. A Discogs-imported library album has no MB release-group-id,
      so it would never light up the "in library" badge on the MB browse
      tab (and vice versa) without a second-chance match. The title
      fallback is the ONLY cross-namespace bridge we have short of a
      manual MB↔Discogs ID mapping service.

      Risk is narrowed in three ways:
        - `library_albums` is already artist-scoped by the caller
          (`srv.get_library_artist(name, mbid)`), so the fallback only
          searches within one artist's catalog, not the whole library.
        - Exact-ID matches take primacy (`lib_by_rgid.get(rid) or …`),
          so title fuzzing only runs when no ID match was found.
        - The `in_library` badge on browse rows is display-only. The
          add-to-pipeline action is gated on the row's own MB/Discogs
          id, not on in_library — so a wrong-match here cannot corrupt
          pipeline state the way #119's fuzzy match did.

      Known accepted failure mode: within one artist, if two legit
      different release groups normalize to the same title (self-titled
      + remaster, EP vs Album of the same name, live vs studio), the
      first library row wins and another row's quality numbers may
      attach to a sibling browse row. The overlay badge is wrong; no
      destructive action follows.

      Revisit (issue #125 left this as by-design): if wrong-badge
      reports show up in practice, the smallest structural change is
      to keep `in_library=True` on title-fallback matches but drop the
      quality fields (`library_format` / `library_min_bitrate` /
      `library_rank`). That sharpens the invariant "quality numbers
      attach only to exact-ID matches" without losing the cross-source
      presence signal.

    rank_fn: optional callable(format_str, bitrate_kbps) -> str. Lets the
    caller plug in the codec-aware quality_rank computation without
    pulling the rank config into this pure module.
    """
    # Map -> matched album dict (not just bool) so we can also set
    # quality fields when a row hits.
    lib_by_rgid: dict[str, dict] = {}
    lib_by_mbid: dict[str, dict] = {}
    # lib_by_title is the cross-source bridge (see docstring). First
    # library row per normalized title wins — deterministic by input
    # order, not "best" by any quality heuristic.
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

    # Exact-ID match primary, title fallback secondary. The `or` ordering
    # is load-bearing — see docstring "Risk is narrowed" notes.
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


def _dedupe_within_source(rows: list[dict]) -> list[dict]:
    """Collapse same-normalized-title + same-year + same-type duplicates.

    Only case-only or punctuation-only title variants of the SAME
    release (same year, same type) collapse — "Twist And Shout" Album
    1964 and "Twist and Shout" Album 1964 become one. Legitimately
    different release groups (EP 1963 vs Album 1964 of the same name,
    or re-releases in different years) stay separate.

    Keep first (input order) as canonical. Missing title: passthrough.
    """
    seen: set[tuple[str, int | None, str]] = set()
    result: list[dict] = []
    for r in rows:
        norm = normalize_title(r.get("title", ""))
        if not norm:
            result.append(r)
            continue
        year = extract_year(r.get("first_release_date", ""))
        type_ = (r.get("type") or "").lower()
        key = (norm, year, type_)
        if key in seen:
            continue
        seen.add(key)
        result.append(r)
    return result


def merge_discographies(
    mb_groups: list[dict],
    discogs_groups: list[dict],
) -> CompareBuckets:
    """Bucket MB + Discogs release groups by exact title+year match.

    Pre-pass: collapse within-source same-title+year+type duplicates
    via _dedupe_within_source so the cross-source merge sees one row
    per logical album per source.

    Match rule:
      1. Normalized title equality is required.
      2. Years must match exactly (or both be unknown for a weak
         title-only fallback). Any year mismatch, even by 1 year, splits
         into two rows — the alternative (tolerance) produced false
         positives like MB "Twist and Shout" Album 1964 matching
         Discogs "Twist And Shout" EP 1963.
      3. Among exact-year candidates, prefer the one with matching type
         (Album↔Album beats Album↔EP) so MB Album 1964 picks Discogs
         Album 1964 over Discogs EP 1964 when both exist.

    Each Discogs entry can match at most one MB entry.
    """
    mb_groups = _dedupe_within_source(mb_groups)
    discogs_groups = _dedupe_within_source(discogs_groups)

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
        m_type = (m.get("type") or "").lower()

        # Score each available candidate; highest wins. A negative score
        # disqualifies. Exact year required (or both unknown).
        best_idx: int | None = None
        best_score = -1
        for di in by_norm.get(norm, []):
            if di in matched:
                continue
            d = discogs_groups[di]
            d_year = extract_year(d.get("first_release_date", ""))
            d_type = (d.get("type") or "").lower()

            if m_year is None and d_year is None:
                score = 1  # weakest — title-only match, years unknown
            elif m_year is not None and d_year is not None and m_year == d_year:
                score = 10  # exact year match
            else:
                # Year mismatch (including partial-unknown). Reject.
                continue

            # Type match bonus — only affects tie-breaking among
            # otherwise-equal candidates.
            if m_type and d_type and m_type == d_type:
                score += 5

            if score > best_score:
                best_score = score
                best_idx = di

        if best_idx is not None:
            matched.add(best_idx)
            both.append({"mb": m, "discogs": discogs_groups[best_idx]})
        else:
            mb_only.append(m)

    discogs_only = [d for i, d in enumerate(discogs_groups) if i not in matched]
    return CompareBuckets(both=both, mb_only=mb_only, discogs_only=discogs_only)
