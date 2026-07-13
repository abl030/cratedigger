"""Fuzzy merge of MB + Discogs artist discographies.

Pure functions — no I/O. The web layer fetches both source's discographies
and hands them to merge_discographies(), which buckets them into:

  - 'both'        : (mb_rg, discogs_master) pairs that appear to be the same
                    logical work (matched on title, provenance, structural
                    type evidence, and a conservative date rule)
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
      - library_avg_bitrate: int (kbps) — current label/rank signal
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
      `library_avg_bitrate` /
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
        # Beets artist projections are bps. Keep the explicit minimum as a
        # floor, while current labels/ranks consume the positive-track mean.
        min_br_bps = match.get("min_bitrate") or 0
        avg_br_bps = match.get("avg_bitrate") or 0
        min_kbps = (min_br_bps // 1000) if min_br_bps else 0
        avg_kbps = (avg_br_bps // 1000) if avg_br_bps else 0
        row["library_format"] = fmt
        row["library_min_bitrate"] = min_kbps
        row["library_avg_bitrate"] = avg_kbps
        if rank_fn:
            row["library_rank"] = rank_fn(fmt, avg_kbps)

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


_STRUCTURAL_TYPES = frozenset({"Album", "EP", "Single"})


def _mb_structural_types(row: dict) -> frozenset[str]:
    """Return recognized MB structural evidence; Other/blank is unknown."""
    type_ = row.get("type")
    return frozenset({type_}) if type_ in _STRUCTURAL_TYPES else frozenset()


def _discogs_structural_types(row: dict) -> frozenset[str]:
    """Return Discogs master-wide type evidence from the mirror contract.

    The legacy scalar ``type`` describes one representative pressing and can
    be misleading. It is display metadata only and must never authorize a
    merge across years or a structural boundary.
    """
    return frozenset(
        type_ for type_ in row.get("primary_types", [])
        if type_ in _STRUCTURAL_TYPES
    )


def _dedupe_type_identity(
    row: dict, *, source: str
) -> tuple[str, frozenset[str] | str]:
    """Use structural evidence, falling back to scalar within one source.

    The scalar fallback only prevents distinct unknown-type rows from being
    collapsed by the within-source pre-pass. It is never cross-source match
    evidence and cannot authorize an adjacent-year pairing.
    """
    structural_types = (
        _mb_structural_types(row)
        if source == "mb"
        else _discogs_structural_types(row)
    )
    if structural_types:
        return ("structural", structural_types)
    return ("scalar-fallback", str(row.get("type") or "").lower())


def _dedupe_within_source(rows: list[dict], *, source: str) -> list[dict]:
    """Collapse duplicates only within one provenance/type identity.

    Only case-only or punctuation-only title variants of the SAME
    release (same year, same structural set and appearance provenance)
    collapse — "Twist And Shout" Album
    1964 and "Twist and Shout" Album 1964 become one. Legitimately
    different release groups (EP 1963 vs Album 1964 of the same name,
    or re-releases in different years) stay separate.

    Keep first (input order) as canonical. Missing title: passthrough.
    """
    seen: set[
        tuple[str, int | None, tuple[str, frozenset[str] | str], bool]
    ] = set()
    result: list[dict] = []
    for r in rows:
        norm = normalize_title(r.get("title", ""))
        if not norm:
            result.append(r)
            continue
        year = extract_year(r.get("first_release_date", ""))
        type_identity = _dedupe_type_identity(r, source=source)
        key = (norm, year, type_identity, bool(r.get("is_appearance")))
        if key in seen:
            continue
        seen.add(key)
        result.append(r)
    return result


def merge_discographies(
    mb_groups: list[dict],
    discogs_groups: list[dict],
) -> CompareBuckets:
    """Bucket MB release groups and Discogs masters conservatively.

    Pre-pass: collapse within-source same-title+year+structural-type duplicates
    via _dedupe_within_source so the cross-source merge sees one row
    per logical album per source.

    Match rule:
      1. Normalized title equality is required.
      2. Appearance provenance must agree. A VA compilation appearance never
         pairs with the artist's mainline work.
      3. Recognized, nonempty structural sets must overlap. MB contributes
         its scalar Album/EP/Single type; Discogs contributes only the
         mirror's master-wide ``primary_types`` set. The legacy Discogs
         scalar is one representative pressing and is not evidence.
      4. Exact known years may pair when types are not known-disjoint. Both
         unknown years retain the weak title fallback. Adjacent known years
         may pair only with positive overlapping structural evidence;
         partial-unknown and larger deltas stay separate.
      5. Candidate rank is exact year, then adjacent year, then both-unknown;
         overlapping evidence breaks ties. Equal ranks keep Discogs input
         order for one MB row; across the whole cohort the stable tie policy
         is MB input order, then Discogs input order.

    The historical Beatles bug came from a blind year tolerance: MB "Twist
    and Shout" Album 1964 paired with a Discogs EP from 1963. Structural
    boundaries now prevent that while allowing "The Pointless Gift" Album
    2000/2001 to pair. This does not split remasters or reissues: MusicBrainz
    release groups and Discogs masters have already grouped their child
    pressings before this display-only cross-source merge runs.

    Each Discogs entry can match at most one MB entry.
    """
    mb_groups = _dedupe_within_source(mb_groups, source="mb")
    discogs_groups = _dedupe_within_source(discogs_groups, source="discogs")

    by_norm: dict[str, list[int]] = defaultdict(list)
    for i, d in enumerate(discogs_groups):
        norm = normalize_title(d.get("title", ""))
        if norm:
            by_norm[norm].append(i)

    candidate_edges: list[tuple[tuple[int, int], int, int]] = []
    for mi, m in enumerate(mb_groups):
        norm = normalize_title(m.get("title", ""))
        m_year = extract_year(m.get("first_release_date", ""))
        m_types = _mb_structural_types(m)
        m_appearance = bool(m.get("is_appearance"))

        for di in by_norm.get(norm, []):
            d = discogs_groups[di]
            d_year = extract_year(d.get("first_release_date", ""))
            d_types = _discogs_structural_types(d)
            if m_appearance != bool(d.get("is_appearance")):
                continue

            type_overlap = bool(m_types & d_types)
            if m_types and d_types and not type_overlap:
                continue

            if m_year is None and d_year is None:
                year_score = 1  # weakest — title-only match, years unknown
            elif m_year is not None and d_year is not None and m_year == d_year:
                year_score = 3
            elif (
                m_year is not None
                and d_year is not None
                and abs(m_year - d_year) == 1
                and m_types
                and d_types
                and type_overlap
            ):
                year_score = 2
            else:
                continue

            score = (year_score, int(type_overlap))
            candidate_edges.append((score, mi, di))

    # Highest-confidence edges claim their rows first across the whole title
    # cohort. This keeps a later MB exact-year edge from losing its Discogs
    # row to an earlier MB adjacent-year edge. Equal confidence is stable by
    # MB input order, then Discogs input order.
    candidate_edges.sort(
        key=lambda edge: (-edge[0][0], -edge[0][1], edge[1], edge[2])
    )
    matched_mb: set[int] = set()
    matched_discogs: set[int] = set()
    mb_to_discogs: dict[int, int] = {}
    for _score, mi, di in candidate_edges:
        if mi in matched_mb or di in matched_discogs:
            continue
        matched_mb.add(mi)
        matched_discogs.add(di)
        mb_to_discogs[mi] = di

    both: list[dict] = []
    mb_only: list[dict] = []
    for mi, m in enumerate(mb_groups):
        if mi in mb_to_discogs:
            both.append({"mb": m, "discogs": discogs_groups[mb_to_discogs[mi]]})
        else:
            mb_only.append(m)

    discogs_only = [
        d for i, d in enumerate(discogs_groups) if i not in matched_discogs
    ]
    return CompareBuckets(both=both, mb_only=mb_only, discogs_only=discogs_only)
